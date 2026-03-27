//! 日志写入器 - 集成段管理的写入组件
//!
//! # 教学价值
//! - 学习组件协作设计
//! - 学习状态管理
//! - 学习资源池化

use super::{FileStorage, SegmentConfig, SegmentManager, Storage, crc32, format};
use crate::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 日志写入器配置
#[derive(Debug, Clone)]
pub struct LogWriterConfig {
    /// 段配置
    pub segment_config: SegmentConfig,
    /// 写入后是否同步
    pub sync_on_write: bool,
    /// 缓冲区大小
    pub buffer_size: usize,
}

impl Default for LogWriterConfig {
    fn default() -> Self {
        Self {
            segment_config: SegmentConfig::default(),
            sync_on_write: false,
            buffer_size: 64 * 1024, // 64KB
        }
    }
}

impl LogWriterConfig {
    pub fn with_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.segment_config = SegmentConfig::new(dir);
        self
    }

    pub fn with_max_segment_size(mut self, size: u64) -> Self {
        self.segment_config = self.segment_config.with_max_size(size);
        self
    }

    pub fn with_sync_on_write(mut self, sync: bool) -> Self {
        self.sync_on_write = sync;
        self
    }
}

/// 写入位置信息
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WritePosition {
    /// 段 ID
    pub segment_id: u64,
    /// 段内偏移量
    pub offset: u64,
    /// 数据长度
    pub length: u64,
}

/// 日志写入器
///
/// 封装段管理和存储操作，提供统一的写入接口。
pub struct LogWriter {
    config: LogWriterConfig,
    segment_manager: RwLock<SegmentManager>,
    active_storage: RwLock<Option<Arc<FileStorage>>>,
}

impl LogWriter {
    /// 创建日志写入器
    pub async fn new(config: LogWriterConfig) -> Result<Self> {
        let segment_manager = SegmentManager::new(config.segment_config.clone())
            .map_err(|e| Error::Generic(format!("Failed to create segment manager: {}", e)))?;

        Ok(Self {
            config,
            segment_manager: RwLock::new(segment_manager),
            active_storage: RwLock::new(None),
        })
    }

    /// 获取或创建活跃段的存储
    async fn get_active_storage(&self) -> Result<Arc<FileStorage>> {
        // 先尝试获取现有存储
        {
            let storage = self.active_storage.read().await;
            if let Some(ref s) = *storage {
                return Ok(s.clone());
            }
        }

        // 需要创建新存储
        let mut manager = self.segment_manager.write().await;

        // 如果没有活跃段，创建第一个
        if manager.active_id() == 0 {
            manager
                .create_segment()
                .map_err(|e| Error::Generic(format!("Failed to create first segment: {}", e)))?;
        }

        let path = manager.active_path();
        let storage = Arc::new(
            FileStorage::new(&path)
                .await
                .map_err(|e| Error::Generic(format!("Failed to create storage: {}", e)))?,
        );

        // 写入段文件头
        storage.write_header_if_empty().await?;

        // 保存到活跃存储
        let mut active = self.active_storage.write().await;
        *active = Some(storage.clone());

        Ok(storage)
    }

    /// 写入数据（带长度前缀和CRC32）
    ///
    /// 格式：[8字节长度][4字节CRC32][数据...]
    ///
    /// # 返回
    /// 返回写入位置信息（offset 为数据开始位置，不含记录头）
    pub async fn write(&self, data: &[u8]) -> Result<WritePosition> {
        // 获取活跃存储
        let storage = self.get_active_storage().await?;

        // 获取段信息
        let segment_id = {
            let manager = self.segment_manager.read().await;
            manager.active_id()
        };

        // 计算数据CRC32
        let data_crc = crc32(data);

        // 写入长度前缀 (8 bytes)
        let length_bytes = (data.len() as u64).to_be_bytes();
        let _offset = storage.append(&length_bytes).await?;

        // 写入CRC32 (4 bytes)
        let crc_bytes = data_crc.to_be_bytes();
        storage.append(&crc_bytes).await?;

        // 写入数据
        let data_offset = storage.append(data).await?;

        // 计算总长度（含记录头）
        let total_len = format::RECORD_HEADER_SIZE + data.len() as u64;

        // 更新段大小，检查是否需要轮转
        let should_rotate = {
            let mut manager = self.segment_manager.write().await;
            manager.update_active_size(total_len)
        };

        // 如果需要轮转，清除活跃存储，强制下次创建新段
        if should_rotate {
            let mut active = self.active_storage.write().await;
            *active = None;
        }

        // 如果配置了 sync_on_write，同步数据
        if self.config.sync_on_write {
            storage.sync().await?;
        }

        Ok(WritePosition {
            segment_id,
            offset: data_offset, // 返回数据开始位置（不含前缀）
            length: data.len() as u64,
        })
    }

    /// 批量写入
    pub async fn write_batch(&self, data_list: &[&[u8]]) -> Result<Vec<WritePosition>> {
        let mut positions = Vec::with_capacity(data_list.len());

        for data in data_list {
            let pos = self.write(data).await?;
            positions.push(pos);
        }

        Ok(positions)
    }

    /// 强制轮转到新段
    pub async fn rotate(&self) -> Result<(u64, std::path::PathBuf)> {
        let mut manager = self.segment_manager.write().await;
        let (id, path) = manager
            .rotate()
            .map_err(|e| Error::Generic(format!("Failed to rotate: {}", e)))?;

        // 清除活跃存储
        let mut active = self.active_storage.write().await;
        *active = None;

        Ok((id, path))
    }

    /// 获取当前活跃段 ID
    pub async fn active_segment_id(&self) -> u64 {
        let manager = self.segment_manager.read().await;
        manager.active_id()
    }

    /// 获取所有段信息
    pub async fn segments(&self) -> Vec<super::SegmentMeta> {
        let manager = self.segment_manager.read().await;
        manager.segments().to_vec()
    }

    /// 同步所有未持久化的数据
    pub async fn sync(&self) -> Result<()> {
        let storage = self.active_storage.read().await;
        if let Some(ref s) = *storage {
            s.sync().await?;
        }
        Ok(())
    }

    /// 关闭写入器
    pub async fn close(&self) -> Result<()> {
        self.sync().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_basic_write() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_max_segment_size(1000);

        let writer = LogWriter::new(config).await.unwrap();

        // 写入数据
        let pos = writer.write(b"hello world").await.unwrap();

        assert_eq!(pos.segment_id, 1);
        // offset = 16 (segment header) + 12 (record header: 8 length + 4 crc)
        assert_eq!(pos.offset, 28);
        assert_eq!(pos.length, 11);
    }

    #[tokio::test]
    async fn test_multiple_writes() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_max_segment_size(1000);

        let writer = LogWriter::new(config).await.unwrap();

        // 多次写入
        writer.write(b"data1").await.unwrap();
        writer.write(b"data2").await.unwrap();
        writer.write(b"data3").await.unwrap();

        let segments = writer.segments().await;
        assert!(!segments.is_empty());
    }

    #[tokio::test]
    async fn test_rotate_on_size_limit() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_max_segment_size(10); // 小 size 便于触发轮转

        let writer = LogWriter::new(config).await.unwrap();

        // 写入超过限制的数据
        writer.write(b"12345678901").await.unwrap(); // 11 bytes

        // 应该已经轮转到新段
        let id = writer.active_segment_id().await;
        assert!(id >= 1);
    }

    #[tokio::test]
    async fn test_sync() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true);

        let writer = LogWriter::new(config).await.unwrap();

        writer.write(b"test data").await.unwrap();

        // sync_on_write 为 true 时写入后已同步
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_manual_rotate() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_max_segment_size(1000);

        let writer = LogWriter::new(config).await.unwrap();

        let id1 = writer.active_segment_id().await;

        // 手动轮转
        let (id2, _path) = writer.rotate().await.unwrap();

        assert!(id2 > id1);
    }

    #[tokio::test]
    async fn test_batch_write() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_max_segment_size(1000);

        let writer = LogWriter::new(config).await.unwrap();

        let data_list: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        let positions = writer.write_batch(&data_list).await.unwrap();

        assert_eq!(positions.len(), 3);
    }
}
