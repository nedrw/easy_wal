//! 日志读取器 - 顺序读取组件
//!
//! # 教学价值
//! - 学习迭代器模式
//! - 学习状态管理
//! - 学习资源清理

use super::{FileStorage, SegmentConfig, SegmentManager, Storage};
use crate::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 读取位置信息
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPosition {
    /// 段 ID
    pub segment_id: u64,
    /// 段内偏移量
    pub offset: u64,
}

/// 日志读取器配置
#[derive(Debug, Clone)]
pub struct LogReaderConfig {
    /// 段配置
    pub segment_config: SegmentConfig,
    /// 批量读取大小
    pub batch_size: usize,
    /// 读取缓冲区大小
    pub read_buffer_size: usize,
}

impl Default for LogReaderConfig {
    fn default() -> Self {
        Self {
            segment_config: SegmentConfig::default(),
            batch_size: 100,
            read_buffer_size: 64 * 1024, // 64KB
        }
    }
}

impl LogReaderConfig {
    pub fn with_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.segment_config = SegmentConfig::new(dir);
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

/// 日志读取器
///
/// 提供顺序读取功能，支持遍历所有段的数据。
pub struct LogReader {
    config: LogReaderConfig,
    segment_manager: RwLock<SegmentManager>,
    /// 当前读取位置
    position: RwLock<ReadPosition>,
    /// 当前活跃存储
    active_storage: RwLock<Option<Arc<FileStorage>>>,
}

impl LogReader {
    /// 创建日志读取器
    pub async fn new(config: LogReaderConfig) -> Result<Self> {
        let mut segment_manager = SegmentManager::new(config.segment_config.clone())
            .map_err(|e| Error::Generic(format!("Failed to create segment manager: {}", e)))?;

        // 如果没有现有段，创建第一个段
        if segment_manager.segments().is_empty() {
            segment_manager.create_segment().ok();
        }

        Ok(Self {
            config,
            segment_manager: RwLock::new(segment_manager),
            position: RwLock::new(ReadPosition {
                segment_id: 1,
                offset: 0,
            }),
            active_storage: RwLock::new(None),
        })
    }

    /// 获取或创建指定段的存储
    async fn get_storage_for_segment(&self, segment_id: u64) -> Result<Arc<FileStorage>> {
        // 先尝试获取现有存储
        {
            let storage = self.active_storage.read().await;
            if let Some(ref s) = *storage {
                let manager = self.segment_manager.read().await;
                if manager.active_id() == segment_id {
                    return Ok(s.clone());
                }
            }
        }

        // 需要加载指定段
        let manager = self.segment_manager.read().await;
        let path = manager
            .segment_path(segment_id)
            .ok_or_else(|| Error::Generic(format!("Segment {} not found", segment_id)))?;

        let storage = Arc::new(
            FileStorage::new(&path)
                .await
                .map_err(|e| Error::Generic(format!("Failed to open storage: {}", e)))?,
        );

        // 保存到活跃存储
        let mut active = self.active_storage.write().await;
        *active = Some(storage.clone());

        Ok(storage)
    }

    /// 读取单条数据
    ///
    /// # 参数
    /// - `offset`: 读取偏移量
    /// - `length`: 读取长度
    ///
    /// # 返回
    /// 读取到的数据
    pub async fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        let pos = self.position.read().await;
        let storage = self.get_storage_for_segment(pos.segment_id).await?;
        storage.read(offset, length).await
    }

    /// 读取指定位置的数据
    pub async fn read_at(&self, segment_id: u64, offset: u64, length: u64) -> Result<Vec<u8>> {
        let storage = self.get_storage_for_segment(segment_id).await?;
        storage.read(offset, length).await
    }

    /// 批量顺序读取
    ///
    /// 从当前位置开始批量读取数据。
    /// 读取完成后会自动更新位置。
    ///
    /// # 参数
    /// - `max_count`: 最大读取条数
    ///
    /// # 返回
    /// 读取到的数据列表
    pub async fn read_batch(&self, max_count: usize) -> Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(max_count);
        let count = max_count.min(self.config.batch_size);

        for _ in 0..count {
            match self.read_next().await {
                Ok(data) => results.push(data),
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(results)
    }

    /// 读取下一条数据
    ///
    /// 从当前位置读取一条数据，并更新位置。
    /// 内部实现了简单的长度前缀格式：前 8 字节为数据长度（大端序）。
    pub async fn read_next(&self) -> Result<Vec<u8>> {
        // 先获取当前位置（释放锁后再做IO）
        let (segment_id, offset) = {
            let pos = self.position.read().await;
            (pos.segment_id, pos.offset)
        };

        let storage = self.get_storage_for_segment(segment_id).await?;

        // 读取长度前缀 (8 bytes)
        let length_bytes = storage.read(offset, 8).await?;
        if length_bytes.is_empty() {
            return Err(Error::Eof);
        }

        let length = u64::from_be_bytes([
            length_bytes[0],
            length_bytes[1],
            length_bytes[2],
            length_bytes[3],
            length_bytes[4],
            length_bytes[5],
            length_bytes[6],
            length_bytes[7],
        ]) as u64;

        // 读取数据
        let data_offset = offset + 8;
        let data = storage.read(data_offset, length).await?;

        // 更新位置（IO完成后再次获取锁）
        let mut write_pos = self.position.write().await;
        write_pos.offset = data_offset + length;

        Ok(data)
    }

    /// 跳到指定位置
    pub async fn seek(&self, segment_id: u64, offset: u64) {
        let mut pos = self.position.write().await;
        pos.segment_id = segment_id;
        pos.offset = offset;
    }

    /// 跳到开头
    pub async fn seek_to_start(&self) {
        self.seek(1, 0).await;
    }

    /// 获取当前位置
    pub async fn position(&self) -> ReadPosition {
        *self.position.read().await
    }

    /// 获取所有段信息
    pub async fn segments(&self) -> Vec<super::SegmentMeta> {
        let manager = self.segment_manager.read().await;
        manager.segments().to_vec()
    }

    /// 获取段数量
    pub async fn segment_count(&self) -> usize {
        let manager = self.segment_manager.read().await;
        manager.segments().len()
    }

    /// 关闭读取器
    pub async fn close(&self) -> Result<()> {
        let mut active = self.active_storage.write().await;
        *active = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LogWriter, LogWriterConfig};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_read_empty_segment() {
        let temp_dir = tempdir().unwrap();
        let config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = LogReader::new(config).await.unwrap();

        let pos = reader.position().await;
        assert_eq!(pos.segment_id, 1);
        assert_eq!(pos.offset, 0);
    }

    #[tokio::test]
    async fn test_seek() {
        let temp_dir = tempdir().unwrap();
        let config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = LogReader::new(config).await.unwrap();

        reader.seek(5, 100).await;

        let pos = reader.position().await;
        assert_eq!(pos.segment_id, 5);
        assert_eq!(pos.offset, 100);
    }

    #[tokio::test]
    async fn test_seek_to_start() {
        let temp_dir = tempdir().unwrap();
        let config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = LogReader::new(config).await.unwrap();

        reader.seek(5, 100).await;
        reader.seek_to_start().await;

        let pos = reader.position().await;
        assert_eq!(pos.segment_id, 1);
        assert_eq!(pos.offset, 0);
    }

    // 注意：读写集成测试在 WalManager 中进行
    // LogReader 的 read_next 使用长度前缀格式，需与 WalManager 配合使用

    #[tokio::test]
    async fn test_segment_count() {
        let temp_dir = tempdir().unwrap();

        let writer_config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_max_segment_size(10) // 小 size 便于触发轮转
            .with_sync_on_write(true);
        let writer = LogWriter::new(writer_config).await.unwrap();

        // 写入数据触发轮转
        writer.write(b"12345678901").await.unwrap(); // 11 bytes
        writer.close().await.unwrap();

        let reader_config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = LogReader::new(reader_config).await.unwrap();

        let count = reader.segment_count().await;
        assert!(count >= 1);
    }
}
