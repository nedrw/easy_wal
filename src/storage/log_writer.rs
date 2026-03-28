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
    /// 缓冲区大小
    pub buffer_size: usize,
}

impl Default for LogWriterConfig {
    fn default() -> Self {
        Self {
            segment_config: SegmentConfig::default(),
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
/// 同步策略由 WriteCoordinator 负责，本组件只执行写入和同步操作。
pub struct LogWriter {
    #[allow(dead_code)]
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
    /// 格式：[4字节 magic][4字节长度][4字节CRC32][数据...]
    ///
    /// 根据同步策略决定是否执行 fsync：
    /// - FsyncOnWrite: 每次写入后自动同步
    /// - Batch: 累积到指定数量后同步
    /// - Periodic: 需要外部定时任务触发
    /// - None: 不同步，依赖操作系统缓冲区
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

        // 同步决策由 WriteCoordinator 负责，本组件只负责写入

        Ok(WritePosition {
            segment_id,
            offset: data_offset, // 返回数据开始位置（不含前缀）
            length: data.len() as u64,
        })
    }

    /// 批量写入
    ///
    /// 使用 storage 层的原子批量追加接口，支持跨段写入。
    /// 单条记录不可拆分跨段，多条记录可在段内批量追加（段内原子）。
    /// 当批量数据超过段大小时，自动轮转到新段继续写入。
    /// 根据同步策略决定是否执行 fsync。
    pub async fn write_batch(&self, data_list: &[&[u8]]) -> Result<Vec<WritePosition>> {
        if data_list.is_empty() {
            return Ok(Vec::new());
        }

        // 准备所有数据（记录头 + 数据）
        let records: Vec<Vec<u8>> = data_list
            .iter()
            .map(|data| {
                let data_crc = crc32(data);
                let mut record =
                    Vec::with_capacity(format::RECORD_HEADER_SIZE as usize + data.len());

                // 记录头: [4B magic][4B length][4B crc]
                record.extend_from_slice(&format::RECORD_MAGIC.to_be_bytes());
                record.extend_from_slice(&(data.len() as u32).to_be_bytes());
                record.extend_from_slice(&data_crc.to_be_bytes());
                // 数据
                record.extend_from_slice(data);

                record
            })
            .collect();

        let mut positions = Vec::with_capacity(data_list.len());
        let mut record_index = 0;

        while record_index < records.len() {
            // 获取当前活跃存储
            let storage = self.get_active_storage().await?;

            // 获取段配置
            let (segment_id, max_segment_size) = {
                let manager = self.segment_manager.read().await;
                (manager.active_id(), manager.config().max_segment_size)
            };

            // 计算当前段剩余空间
            let current_size = storage.size().await?;
            let header_size = format::SEGMENT_HEADER_SIZE;
            let remaining = if current_size < header_size {
                max_segment_size.saturating_sub(header_size)
            } else {
                max_segment_size.saturating_sub(current_size - header_size)
            };

            // 找出当前段能容纳的记录（贪心填充）
            let mut batch_records: Vec<&[u8]> = Vec::new();
            let mut batch_size: u64 = 0;
            let mut batch_indices: Vec<usize> = Vec::new();

            while record_index < records.len() {
                let record_len = records[record_index].len() as u64;
                // 单条记录不能超过段最大大小（否则永远无法写入）
                if record_len > max_segment_size - header_size {
                    return Err(Error::Generic(format!(
                        "Record size {} exceeds maximum segment capacity",
                        record_len
                    )));
                }
                // 检查是否能容纳下一条记录
                if batch_size + record_len > remaining && !batch_records.is_empty() {
                    break; // 当前段满了，停止填充
                }
                batch_records.push(records[record_index].as_slice());
                batch_indices.push(record_index);
                batch_size += record_len;
                record_index += 1;
            }

            // 批量追加到当前段（段内原子）
            let offsets = storage.append_batch(&batch_records).await?;

            // 更新段大小
            {
                let mut manager = self.segment_manager.write().await;
                manager.update_active_size(batch_size);
            }

            // 构建当前位置信息
            for (i, &offset) in offsets.iter().enumerate() {
                let data_idx = batch_indices[i];
                positions.push(WritePosition {
                    segment_id,
                    offset: offset + format::RECORD_HEADER_SIZE,
                    length: data_list[data_idx].len() as u64,
                });
            }

            // 检查是否需要轮转到新段
            let needs_rotate = {
                let manager = self.segment_manager.read().await;
                manager.should_rotate()
            };

            if needs_rotate {
                // 轮转到新段
                let mut manager = self.segment_manager.write().await;
                manager
                    .rotate()
                    .map_err(|e| Error::Generic(format!("Failed to rotate: {}", e)))?;

                // 清除活跃存储
                let mut active = self.active_storage.write().await;
                *active = None;
            }
        }

        // 同步决策由 WriteCoordinator 负责，本组件只负责写入

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
    /// 执行 fsync，确保数据持久化到磁盘。
    /// 同步决策由 WriteCoordinator 负责，本方法只执行实际的 fsync 操作。
    pub async fn sync(&self) -> Result<()> {
        let storage = self.active_storage.read().await;
        if let Some(ref s) = *storage {
            s.sync().await?;
        }
        Ok(())
    }

    /// 关闭写入器
    ///
    /// 执行最后的同步操作。
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
        // offset = 16 (segment header) + 12 (record header: 4 magic + 4 length + 4 crc)
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
        let config = LogWriterConfig::default().with_dir(temp_dir.path());

        let writer = LogWriter::new(config).await.unwrap();

        writer.write(b"test data").await.unwrap();
        writer.sync().await.unwrap();
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
            .with_max_segment_size(10000);

        let writer = LogWriter::new(config).await.unwrap();

        let data_list: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        let positions = writer.write_batch(&data_list).await.unwrap();

        assert_eq!(positions.len(), 3);
        assert_eq!(positions[0].length, 1);
        assert_eq!(positions[1].length, 2);
        assert_eq!(positions[2].length, 3);
    }

    #[tokio::test]
    async fn test_batch_write_cross_segment() {
        let temp_dir = tempdir().unwrap();
        // 设置小段大小以便触发跨段
        let config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_max_segment_size(50); // 每个段最多50字节

        let writer = LogWriter::new(config).await.unwrap();

        // 准备数据：每条记录约20字节，3条约60字节，需要跨段
        let data_list: Vec<&[u8]> = vec![
            b"12345678901234", // 14 bytes data + 12 bytes header = 26 bytes
            b"12345678901234", // 26 bytes
            b"12345678901234", // 26 bytes (总计78字节，超过50字节段限制)
        ];

        let positions = writer.write_batch(&data_list).await.unwrap();

        assert_eq!(positions.len(), 3);

        // 验证产生了多个段
        let segments = writer.segments().await;
        assert!(
            segments.len() >= 2,
            "Expected at least 2 segments, got {}",
            segments.len()
        );

        // 验证位置信息：前两条在同一段，第三条在新区段
        assert_eq!(positions[0].segment_id, 1);
        assert_eq!(positions[1].segment_id, 1);
        assert_eq!(positions[2].segment_id, 2);
    }
}
