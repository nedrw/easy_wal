//! 日志写入器 - 集成段管理的写入组件
//!
//! # 教学价值
//! - 学习组件协作设计
//! - 学习状态管理
//! - 学习资源池化

use super::{FileStorage, SegmentConfig, Storage, crc32, format, log_segment::WritePosition};
use crate::prelude::*;
use std::path::Path;
use std::sync::Arc;

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

// WritePosition 已移至 log_segment.rs（Kafka 模式统一）
// 这里通过 use super::log_segment::WritePosition 导入

/// 日志写入器（轻量级版本）
///
/// 只负责纯粹的数据写入，不管理段生命周期。
/// 段轮转策略由上层的 SegmentCoordinator 负责。
pub struct LogWriter {
    /// 存储层
    storage: Arc<FileStorage>,
    /// 段 ID
    segment_id: u64,
}

impl LogWriter {
    /// 创建日志写入器
    ///
    /// 接受外部提供的存储和段 ID，不负责段管理。
    pub fn new(storage: Arc<FileStorage>, segment_id: u64) -> Self {
        Self {
            storage,
            segment_id,
        }
    }

    /// 获取存储引用
    pub fn storage(&self) -> &FileStorage {
        &self.storage
    }

    /// 获取段 ID
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// 写入数据（带长度前缀和CRC32）
    ///
    /// 格式：[4字节 magic][4字节长度][4字节CRC32][数据...]
    ///
    /// 注意：本方法只负责纯粹的数据写入，不决策段轮转。
    /// 段轮转策略由上层的 SegmentCoordinator 负责。
    ///
    /// # 返回
    /// 返回写入位置信息（offset 为数据开始位置，不含记录头）
    pub async fn write(&self, data: &[u8]) -> Result<WritePosition> {
        // 计算数据CRC32
        let data_crc = crc32(data);

        // 准备记录头 (12 bytes): [4B magic][4B length][4B crc]
        let magic_bytes = format::RECORD_MAGIC.to_be_bytes();
        let length_bytes = (data.len() as u32).to_be_bytes();
        let crc_bytes = data_crc.to_be_bytes();

        // 原子批量写入：一次性写入整个记录（magic + length + crc + data）
        // 保证并发安全：多个并发 write() 调用时，每个记录完整写入，不会交错
        let data_list: Vec<&[u8]> = vec![&magic_bytes, &length_bytes, &crc_bytes, data];
        let offsets = self.storage.append_batch(&data_list).await?;

        // offsets[3] 是数据的起始位置
        let data_offset = offsets[3];

        Ok(WritePosition {
            segment_id: self.segment_id,
            offset: data_offset, // 返回数据开始位置（不含前缀）
            length: data.len() as u64,
        })
    }

    /// 批量写入（简化版本，不支持跨段）
    ///
    /// 使用 storage 层的原子批量追加接口。
    /// 单条记录不可拆分跨段，多条记录可在段内批量追加（段内原子）。
    ///
    /// 注意：本方法假设当前段有足够空间容纳所有数据。
    /// 如果空间不足，会返回错误。段轮转策略由上层的 SegmentCoordinator 负责。
    ///
    /// TODO: 未来版本可能需要支持跨段批量写入，但这需要更复杂的协调逻辑。
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

        // 批量追加到当前段（段内原子）
        let batch_records: Vec<&[u8]> = records.iter().map(|r| r.as_slice()).collect();
        let offsets = self.storage.append_batch(&batch_records).await?;

        // 构建位置信息
        let positions: Vec<WritePosition> = offsets
            .iter()
            .enumerate()
            .map(|(i, &offset)| WritePosition {
                segment_id: self.segment_id,
                offset: offset + format::RECORD_HEADER_SIZE,
                length: data_list[i].len() as u64,
            })
            .collect();

        Ok(positions)
    }

    /// 获取当前段大小
    pub async fn size(&self) -> Result<u64> {
        self.storage.size().await
    }

    /// 同步所有未持久化的数据
    ///
    /// 执行 fsync，确保数据持久化到磁盘。
    /// 同步决策由 WriteCoordinator 负责，本方法只执行实际的 fsync 操作。
    pub async fn sync(&self) -> Result<()> {
        self.storage.sync().await
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

    /// 辅助函数：创建测试用的 LogWriter
    async fn create_test_writer(dir: &std::path::Path, segment_id: u64) -> LogWriter {
        let path = dir.join(format!("segment{}.wal", segment_id));
        let storage = Arc::new(FileStorage::new(&path).await.unwrap());
        storage.write_header_if_empty().await.unwrap();
        LogWriter::new(storage, segment_id)
    }

    #[tokio::test]
    async fn test_basic_write() {
        let temp_dir = tempdir().unwrap();
        let writer = create_test_writer(temp_dir.path(), 1).await;

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
        let writer = create_test_writer(temp_dir.path(), 1).await;

        // 多次写入
        let pos1 = writer.write(b"data1").await.unwrap();
        let pos2 = writer.write(b"data2").await.unwrap();
        let pos3 = writer.write(b"data3").await.unwrap();

        // 验证所有写入都在同一段
        assert_eq!(pos1.segment_id, 1);
        assert_eq!(pos2.segment_id, 1);
        assert_eq!(pos3.segment_id, 1);

        // 验证段大小增长
        let size = writer.size().await.unwrap();
        assert!(size > 16); // 至少包含段头
    }

    #[tokio::test]
    async fn test_sync() {
        let temp_dir = tempdir().unwrap();
        let writer = create_test_writer(temp_dir.path(), 1).await;

        writer.write(b"test data").await.unwrap();
        writer.sync().await.unwrap();
        writer.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_write() {
        let temp_dir = tempdir().unwrap();
        let writer = create_test_writer(temp_dir.path(), 1).await;

        let data_list: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        let positions = writer.write_batch(&data_list).await.unwrap();

        assert_eq!(positions.len(), 3);
        assert_eq!(positions[0].length, 1);
        assert_eq!(positions[1].length, 2);
        assert_eq!(positions[2].length, 3);
        assert_eq!(positions[0].segment_id, 1);
    }

    #[tokio::test]
    async fn test_segment_id() {
        let temp_dir = tempdir().unwrap();
        let writer = create_test_writer(temp_dir.path(), 5).await;

        // 验证段 ID 正确
        assert_eq!(writer.segment_id(), 5);
    }

    #[tokio::test]
    async fn test_size_tracking() {
        let temp_dir = tempdir().unwrap();
        let writer = create_test_writer(temp_dir.path(), 1).await;

        // 初始大小（只有段头）
        let initial_size = writer.size().await.unwrap();
        assert_eq!(initial_size, 16);

        // 写入后大小增长
        writer.write(b"test").await.unwrap();
        let new_size = writer.size().await.unwrap();
        assert_eq!(new_size, 16 + 12 + 4); // 段头 + 记录头 + 数据
    }
}
