//! 日志写入器 - 集成段管理的写入组件
//!
//! # 教学价值
//! - 学习组件协作设计
//! - 学习状态管理
//! - 学习资源池化
//! - 学习同步策略集成

use super::{FileStorage, SegmentConfig, SegmentManager, Storage, crc32, format};
use crate::prelude::*;
use crate::wal::SyncMode;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 日志写入器配置
#[derive(Debug, Clone)]
pub struct LogWriterConfig {
    /// 段配置
    pub segment_config: SegmentConfig,
    /// 同步模式
    pub sync_mode: SyncMode,
    /// 缓冲区大小
    pub buffer_size: usize,
}

impl Default for LogWriterConfig {
    fn default() -> Self {
        Self {
            segment_config: SegmentConfig::default(),
            sync_mode: SyncMode::None,
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

    /// 设置同步模式
    pub fn with_sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = mode;
        self
    }

    /// 兼容旧 API：设置是否每次写入后同步
    pub fn with_sync_on_write(mut self, sync: bool) -> Self {
        self.sync_mode = if sync {
            SyncMode::FsyncOnWrite
        } else {
            SyncMode::None
        };
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
/// 支持多种同步策略，在性能和数据安全性之间取得平衡。
pub struct LogWriter {
    config: LogWriterConfig,
    segment_manager: RwLock<SegmentManager>,
    active_storage: RwLock<Option<Arc<FileStorage>>>,
    /// 同步策略
    sync_strategy: RwLock<crate::wal::SyncStrategy>,
}

impl LogWriter {
    /// 创建日志写入器
    pub async fn new(config: LogWriterConfig) -> Result<Self> {
        let segment_manager = SegmentManager::new(config.segment_config.clone())
            .map_err(|e| Error::Generic(format!("Failed to create segment manager: {}", e)))?;

        let sync_strategy = crate::wal::SyncStrategy::new(config.sync_mode);

        Ok(Self {
            config,
            segment_manager: RwLock::new(segment_manager),
            active_storage: RwLock::new(None),
            sync_strategy: RwLock::new(sync_strategy),
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
    /// 根据配置的同步策略决定何时执行 fsync：
    /// - None: 不主动同步，依赖操作系统
    /// - FsyncOnWrite: 每次写入后同步
    /// - Batch: 每 N 次写入后同步
    /// - Periodic: 按时间间隔同步
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

        // 写入记录头 (12 bytes): [4B magic][4B length][4B crc]
        let magic_bytes = format::RECORD_MAGIC.to_be_bytes();
        let length_bytes = (data.len() as u32).to_be_bytes();
        let crc_bytes = data_crc.to_be_bytes();

        let _offset = storage.append(&magic_bytes).await?;
        storage.append(&length_bytes).await?;
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

        // 使用 SyncStrategy 决定是否同步
        let should_sync = {
            let mut strategy = self.sync_strategy.write().await;
            strategy.on_write(total_len).await
        };

        if should_sync.is_some() {
            let start = std::time::Instant::now();
            storage.sync().await?;
            let duration_ms = start.elapsed().as_millis() as u64;

            // 记录同步统计
            let mut strategy = self.sync_strategy.write().await;
            strategy.on_synced(total_len, duration_ms).await;
        }

        Ok(WritePosition {
            segment_id,
            offset: data_offset, // 返回数据开始位置（不含前缀）
            length: data.len() as u64,
        })
    }

    /// 批量写入
    ///
    /// 所有数据写入后，根据同步策略决定是否执行一次同步。
    /// 对于 Batch 模式，这表示一批写入，只会计数一次。
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
    ///
    /// 强制立即执行 fsync，并更新同步策略状态。
    pub async fn sync(&self) -> Result<()> {
        let storage = self.active_storage.read().await;
        if let Some(ref s) = *storage {
            let start = std::time::Instant::now();
            s.sync().await?;
            let duration_ms = start.elapsed().as_millis() as u64;

            // 获取待同步字节数并更新策略状态
            let pending_bytes = {
                let strategy = self.sync_strategy.read().await;
                strategy.pending_bytes()
            };

            let mut strategy = self.sync_strategy.write().await;
            strategy.on_synced(pending_bytes, duration_ms).await;
        }
        Ok(())
    }

    /// 获取同步统计信息
    pub async fn sync_stats(&self) -> crate::wal::SyncStats {
        let strategy = self.sync_strategy.read().await;
        strategy.stats().await
    }

    /// 获取当前同步模式
    pub fn sync_mode(&self) -> SyncMode {
        self.config.sync_mode
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
