//! 日志段 - 统一的读写组件（Kafka 模式）
//!
//! # 设计理念
//! - 合并 LogWriter 和 LogReader 的功能
//! - 单一段内的读写操作，不包含段切换逻辑
//! - 状态一致性：读写共享同一个 FileStorage 和 write_position
//!
//! # 架构原则
//! - 组件层：只负责段内操作，不决策段切换
//! - 协调层：负责段切换、位置管理等决策

use super::{FileStorage, Storage, crc32, format};
use crate::prelude::*;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 写入位置信息
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WritePosition {
    /// 段 ID
    pub segment_id: u64,
    /// 段内偏移量（数据开始位置，不含记录头）
    pub offset: u64,
    /// 数据长度
    pub length: u64,
}

/// 读取结果（包含下一个读取位置）
#[derive(Debug, Clone)]
pub struct ReadNextResult {
    /// 读取到的数据
    pub data: Vec<u8>,
    /// 下一条记录的偏移量
    pub next_offset: u64,
}

/// 批量读取结果
#[derive(Debug, Clone)]
pub struct ReadBatchResult {
    /// 读取到的数据列表
    pub records: Vec<Vec<u8>>,
    /// 下一条记录的偏移量
    pub next_offset: u64,
}

/// 原始数据读取结果
#[derive(Debug, Clone)]
pub struct ReadRawResult {
    /// 读取到的原始数据
    pub data: Vec<u8>,
    /// 读取结束位置
    pub end_offset: u64,
}

/// 日志段（Kafka 模式）
///
/// 代表单个段文件，提供读写功能。
/// 读写共享同一个 FileStorage 和 write_position。
pub struct LogSegment {
    /// 段 ID
    segment_id: u64,
    /// 存储层
    storage: Arc<FileStorage>,
    /// 当前写入位置（读写共享）
    write_position: RwLock<u64>,
}

impl LogSegment {
    /// 创建新的日志段
    ///
    /// # 参数
    /// - `storage`: 文件存储
    /// - `segment_id`: 段 ID
    pub fn new(storage: Arc<FileStorage>, segment_id: u64) -> Self {
        Self {
            segment_id,
            storage,
            write_position: RwLock::new(format::SEGMENT_HEADER_SIZE), // 从段头之后开始
        }
    }

    /// 从现有文件创建日志段
    ///
    /// # 参数
    /// - `storage`: 文件存储（已存在的文件）
    /// - `segment_id`: 段 ID
    pub async fn from_existing(storage: Arc<FileStorage>, segment_id: u64) -> Result<Self> {
        // 获取当前文件大小作为写入位置
        let size = storage.size().await?;

        Ok(Self {
            segment_id,
            storage,
            write_position: RwLock::new(size),
        })
    }

    /// 获取段 ID
    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    /// 获取存储引用
    pub fn storage(&self) -> &FileStorage {
        &self.storage
    }

    // ========== 写入功能（从 LogWriter 继承）==========

    /// 追加数据（带长度前缀和CRC32）
    ///
    /// 格式：[4字节 magic][4字节长度][4字节CRC32][数据...]
    ///
    /// # 返回
    /// 返回写入位置信息（offset 为数据开始位置，不含记录头）
    pub async fn append(&self, data: &[u8]) -> Result<WritePosition> {
        // 计算数据CRC32
        let data_crc = crc32(data);

        // 准备记录头 (12 bytes): [4B magic][4B length][4B crc]
        let magic_bytes = format::RECORD_MAGIC.to_be_bytes();
        let length_bytes = (data.len() as u32).to_be_bytes();
        let crc_bytes = data_crc.to_be_bytes();

        // 原子批量写入：一次性写入整个记录（magic + length + crc + data）
        let data_list: Vec<&[u8]> = vec![&magic_bytes, &length_bytes, &crc_bytes, data];
        let offsets = self.storage.append_batch(&data_list).await?;

        // 更新写入位置
        let data_offset = offsets[3];
        let _record_size = format::RECORD_HEADER_SIZE + data.len() as u64;

        {
            let mut write_pos = self.write_position.write().await;
            *write_pos = data_offset + data.len() as u64;
        }

        Ok(WritePosition {
            segment_id: self.segment_id,
            offset: data_offset,
            length: data.len() as u64,
        })
    }

    /// 批量追加（简化版本，不支持跨段）
    ///
    /// 使用 storage 层的原子批量追加接口。
    /// 单条记录不可拆分跨段，多条记录可在段内批量追加。
    ///
    /// # 参数
    /// - `data_list`: 数据列表
    ///
    /// # 返回
    /// 返回每条记录的写入位置
    pub async fn append_batch(&self, data_list: &[&[u8]]) -> Result<Vec<WritePosition>> {
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

        // 更新写入位置
        let last_offset = offsets.last().unwrap();
        let last_data_len = data_list.last().unwrap().len() as u64;

        {
            let mut write_pos = self.write_position.write().await;
            *write_pos = last_offset + format::RECORD_HEADER_SIZE + last_data_len;
        }

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

    /// 同步所有未持久化的数据
    ///
    /// 执行 fsync，确保数据持久化到磁盘。
    pub async fn sync(&self) -> Result<()> {
        self.storage.sync().await
    }

    /// 获取当前段大小
    pub async fn size(&self) -> Result<u64> {
        self.storage.size().await
    }

    /// 获取当前写入位置
    pub async fn write_position(&self) -> u64 {
        *self.write_position.read().await
    }

    // ========== 读取功能（从 LogReader 继承，但移除段切换逻辑）==========

    /// 读取指定位置的数据
    ///
    /// # 参数
    /// - `offset`: 数据起始偏移量
    /// - `length`: 数据长度
    pub async fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        self.storage.read(offset, length).await
    }

    /// 读取下一条记录
    ///
    /// 从指定偏移量读取一条完整的记录。
    /// 格式：[4B Magic][4B Length][4B CRC32][Data...]
    ///
    /// # 参数
    /// - `offset`: 记录起始偏移量
    ///
    /// # 返回
    /// 返回数据和下一条记录的偏移量
    ///
    /// # 错误
    /// - 到达段末尾时返回 Error::Eof
    /// - 数据损坏时返回相应错误
    pub async fn read_next(&self, offset: u64) -> Result<ReadNextResult> {
        // 预读记录头 (12 bytes: 4 magic + 4 length + 4 crc)
        let header = self
            .storage
            .read(offset, format::RECORD_HEADER_SIZE)
            .await?;

        // 如果读取不足记录头大小，说明到达段末尾
        if header.len() < format::RECORD_HEADER_SIZE as usize {
            return Err(Error::Eof);
        }

        // 解析 Magic
        let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        if magic != format::RECORD_MAGIC {
            return Err(Error::Generic(format!(
                "Invalid record magic: {:08x}",
                magic
            )));
        }

        // 解析 Length
        let length = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as u64;

        // 验证长度合理性
        if length == 0 || length > format::MAX_RECORD_SIZE {
            return Err(Error::Generic(format!("Invalid record length: {}", length)));
        }

        // 解析 CRC32
        let expected_crc = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);

        // 读取数据
        let data_offset = offset + format::RECORD_HEADER_SIZE;
        let data = self.storage.read(data_offset, length).await?;

        // 完整性保护：验证实际读取的字节数
        if data.len() as u64 != length {
            return Err(Error::Generic(format!(
                "Incomplete read: expected {} bytes, got {}",
                length,
                data.len()
            )));
        }

        // 验证 CRC32
        let actual_crc = crc32(&data);
        if actual_crc != expected_crc {
            return Err(Error::Generic(format!(
                "CRC32 mismatch: expected {:08x}, got {:08x}",
                expected_crc, actual_crc
            )));
        }

        // 计算下一条记录的偏移量
        let next_offset = data_offset + length;

        Ok(ReadNextResult { data, next_offset })
    }

    /// 批量读取记录
    ///
    /// 从指定偏移量开始，批量读取多条记录。
    ///
    /// # 参数
    /// - `offset`: 起始偏移量
    /// - `max_count`: 最大读取条数
    ///
    /// # 返回
    /// 返回记录列表和下一条记录的偏移量
    pub async fn read_batch(&self, offset: u64, max_count: usize) -> Result<ReadBatchResult> {
        let mut records = Vec::with_capacity(max_count);
        let mut current_offset = offset;

        for _ in 0..max_count {
            match self.read_next(current_offset).await {
                Ok(result) => {
                    records.push(result.data);
                    current_offset = result.next_offset;
                }
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(ReadBatchResult {
            records,
            next_offset: current_offset,
        })
    }

    /// 读取原始数据（不解析格式）
    ///
    /// 用于预读优化，直接从存储读取原始字节。
    /// 只从当前段读取，不会跨段。
    ///
    /// # 参数
    /// - `offset`: 起始偏移量
    /// - `length`: 读取长度
    ///
    /// # 返回
    /// 返回读取到的数据和结束偏移量
    pub async fn read_raw(&self, offset: u64, length: usize) -> Result<ReadRawResult> {
        let data = self.storage.read(offset, length as u64).await?;
        let end_offset = offset + data.len() as u64;

        Ok(ReadRawResult { data, end_offset })
    }

    // ========== 辅助方法 ==========

    /// 检查偏移量是否有效（在段范围内）
    pub async fn is_valid_offset(&self, offset: u64) -> bool {
        let size = match self.size().await {
            Ok(s) => s,
            Err(_) => return false,
        };
        offset >= format::SEGMENT_HEADER_SIZE && offset < size
    }

    /// 获取段文件路径
    pub fn path(&self) -> &std::path::Path {
        self.storage.path()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// 辅助函数：创建测试用的 LogSegment
    async fn create_test_segment(dir: &std::path::Path, segment_id: u64) -> LogSegment {
        let path = dir.join(format!("segment{}.wal", segment_id));
        let storage = Arc::new(FileStorage::new(&path).await.unwrap());
        storage.write_header_if_empty().await.unwrap();
        LogSegment::new(storage, segment_id)
    }

    #[tokio::test]
    async fn test_basic_append_and_read() {
        let temp_dir = tempdir().unwrap();
        let segment = create_test_segment(temp_dir.path(), 1).await;

        // 写入数据
        let pos = segment.append(b"hello world").await.unwrap();
        assert_eq!(pos.segment_id, 1);
        assert_eq!(pos.length, 11);

        // 读取数据
        let result = segment
            .read_next(format::SEGMENT_HEADER_SIZE)
            .await
            .unwrap();
        assert_eq!(result.data, b"hello world");
        assert_eq!(result.next_offset, pos.offset + pos.length);
    }

    #[tokio::test]
    async fn test_multiple_appends() {
        let temp_dir = tempdir().unwrap();
        let segment = create_test_segment(temp_dir.path(), 1).await;

        // 写入多条记录
        let pos1 = segment.append(b"record1").await.unwrap();
        let pos2 = segment.append(b"record2").await.unwrap();
        let pos3 = segment.append(b"record3").await.unwrap();

        // 验证位置递增
        assert!(pos2.offset > pos1.offset);
        assert!(pos3.offset > pos2.offset);

        // 顺序读取
        let result1 = segment
            .read_next(format::SEGMENT_HEADER_SIZE)
            .await
            .unwrap();
        assert_eq!(result1.data, b"record1");

        let result2 = segment.read_next(result1.next_offset).await.unwrap();
        assert_eq!(result2.data, b"record2");

        let result3 = segment.read_next(result2.next_offset).await.unwrap();
        assert_eq!(result3.data, b"record3");
    }

    #[tokio::test]
    async fn test_batch_append() {
        let temp_dir = tempdir().unwrap();
        let segment = create_test_segment(temp_dir.path(), 1).await;

        let data_list: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        let positions = segment.append_batch(&data_list).await.unwrap();

        assert_eq!(positions.len(), 3);
        assert_eq!(positions[0].length, 1);
        assert_eq!(positions[1].length, 2);
        assert_eq!(positions[2].length, 3);
    }

    #[tokio::test]
    async fn test_batch_read() {
        let temp_dir = tempdir().unwrap();
        let segment = create_test_segment(temp_dir.path(), 1).await;

        // 写入多条记录
        for i in 0..5 {
            segment
                .append(format!("record{}", i).as_bytes())
                .await
                .unwrap();
        }

        // 批量读取
        let result = segment
            .read_batch(format::SEGMENT_HEADER_SIZE, 3)
            .await
            .unwrap();
        assert_eq!(result.records.len(), 3);
        assert_eq!(result.records[0], b"record0");
        assert_eq!(result.records[1], b"record1");
        assert_eq!(result.records[2], b"record2");

        // 继续读取
        let result2 = segment.read_batch(result.next_offset, 3).await.unwrap();
        assert_eq!(result2.records.len(), 2); // 只剩2条
        assert_eq!(result2.records[0], b"record3");
        assert_eq!(result2.records[1], b"record4");
    }

    #[tokio::test]
    async fn test_read_raw() {
        let temp_dir = tempdir().unwrap();
        let segment = create_test_segment(temp_dir.path(), 1).await;

        // 写入数据
        segment.append(b"test data").await.unwrap();

        // 读取原始数据
        let result = segment
            .read_raw(format::SEGMENT_HEADER_SIZE, 100)
            .await
            .unwrap();
        assert!(!result.data.is_empty());
        assert!(result.data.len() >= 20); // 至少包含记录头 + 数据
    }

    #[tokio::test]
    async fn test_write_position_tracking() {
        let temp_dir = tempdir().unwrap();
        let segment = create_test_segment(temp_dir.path(), 1).await;

        // 初始写入位置应该是段头之后
        let initial_pos = segment.write_position().await;
        assert_eq!(initial_pos, format::SEGMENT_HEADER_SIZE);

        // 写入后位置应该更新
        segment.append(b"test").await.unwrap();
        let new_pos = segment.write_position().await;
        assert!(new_pos > initial_pos);
    }

    #[tokio::test]
    async fn test_is_valid_offset() {
        let temp_dir = tempdir().unwrap();
        let segment = create_test_segment(temp_dir.path(), 1).await;

        // 写入一些数据
        segment.append(b"test").await.unwrap();

        // 段头之前的偏移量无效
        assert!(!segment.is_valid_offset(0).await);
        assert!(!segment.is_valid_offset(15).await);

        // 段头之后的偏移量有效
        assert!(segment.is_valid_offset(format::SEGMENT_HEADER_SIZE).await);
    }

    #[tokio::test]
    async fn test_sync() {
        let temp_dir = tempdir().unwrap();
        let segment = create_test_segment(temp_dir.path(), 1).await;

        segment.append(b"test data").await.unwrap();
        segment.sync().await.unwrap();
    }
}
