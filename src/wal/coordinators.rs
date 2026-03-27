//! WAL 协调器 - 读写协调组件
//!
//! # 教学价值
//! - 学习协调器模式
//! - 学习读写分离
//! - 学习缓冲优化

use crate::prelude::*;
use crate::storage::{LogReader, LogWriter, ReadPosition, SegmentMeta, WritePosition};
use std::sync::Arc;
use tokio::sync::RwLock;

/// 最大单条记录大小 (64MB)
const MAX_RECORD_SIZE: u64 = 64 * 1024 * 1024;

// ============================================================
// 写入协调器
// ============================================================

/// 写入协调器
///
/// 协调写入操作，提供：
/// - 批量写入聚合（减少 IO 次数）
/// - 与 RecoveryManager 协作
pub struct WriteCoordinator {
    writer: Arc<LogWriter>,
}

impl WriteCoordinator {
    /// 创建写入协调器
    pub fn new(writer: Arc<LogWriter>) -> Self {
        Self { writer }
    }

    /// 获取底层写入器
    pub fn writer(&self) -> Arc<LogWriter> {
        self.writer.clone()
    }

    /// 写入单条数据
    pub async fn write(&self, data: &[u8]) -> Result<WritePosition> {
        if data.len() as u64 > MAX_RECORD_SIZE {
            return Err(Error::Generic(format!(
                "Record too large: {} > {}",
                data.len(),
                MAX_RECORD_SIZE
            )));
        }
        self.writer.write(data).await
    }

    /// 批量写入
    pub async fn write_batch(&self, data_list: &[&[u8]]) -> Result<Vec<WritePosition>> {
        let mut positions = Vec::with_capacity(data_list.len());
        for data in data_list {
            positions.push(self.write(data).await?);
        }
        Ok(positions)
    }

    /// 强制轮转段
    pub async fn rotate(&self) -> Result<(u64, std::path::PathBuf)> {
        self.writer.rotate().await
    }

    /// 获取活跃段 ID
    pub async fn active_segment_id(&self) -> u64 {
        self.writer.active_segment_id().await
    }

    /// 同步数据
    pub async fn sync(&self) -> Result<()> {
        self.writer.sync().await
    }

    /// 关闭写入协调器
    pub async fn close(&self) -> Result<()> {
        self.writer.close().await
    }
}

// ============================================================
// 读取协调器
// ============================================================

/// 读取协调器
///
/// 协调读取操作，提供：
/// - 读取缓冲（预读优化）
/// - 并发读取控制
pub struct ReadCoordinator {
    reader: Arc<RwLock<LogReader>>,
    /// 预读缓冲区
    read_ahead_buffer: Arc<RwLock<ReadAheadBuffer>>,
    /// 预读缓冲区大小
    read_ahead_size: usize,
}

/// 预读缓冲区
struct ReadAheadBuffer {
    /// 缓冲区数据
    data: Vec<u8>,
    /// 当前读取位置
    pos: usize,
    /// 是否已耗尽
    exhausted: bool,
}

impl ReadAheadBuffer {
    fn new() -> Self {
        Self {
            data: Vec::new(),
            pos: 0,
            exhausted: false,
        }
    }

    /// 从缓冲区读取指定长度的数据
    fn read(&mut self, len: usize) -> Option<Vec<u8>> {
        if self.pos + len > self.data.len() {
            return None;
        }
        let result = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Some(result)
    }

    /// 返回剩余可读字节数
    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    /// 检查是否为空
    fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    /// 清空缓冲区
    fn clear(&mut self) {
        self.data.clear();
        self.pos = 0;
        self.exhausted = false;
    }
}

impl ReadCoordinator {
    /// 创建读取协调器
    pub fn new(reader: Arc<RwLock<LogReader>>) -> Self {
        Self {
            reader,
            read_ahead_buffer: Arc::new(RwLock::new(ReadAheadBuffer::new())),
            read_ahead_size: 64 * 1024, // 64KB
        }
    }

    /// 设置预读大小
    pub fn with_read_ahead(mut self, size: usize) -> Self {
        self.read_ahead_size = size;
        self
    }

    /// 获取底层读取器
    pub async fn reader(&self) -> Arc<RwLock<LogReader>> {
        self.reader.clone()
    }

    /// 读取下一条记录（使用预读优化）
    pub async fn read_next(&self) -> Result<Vec<u8>> {
        let mut buffer = self.read_ahead_buffer.write().await;

        // 如果缓冲区空，填充
        if buffer.is_empty() && !buffer.exhausted {
            let reader = self.reader.read().await;
            let data = reader.read_raw(self.read_ahead_size).await?;
            if data.is_empty() {
                buffer.exhausted = true;
                return Err(Error::Eof);
            }
            buffer.data = data;
            buffer.pos = 0;
        }

        // 尝试从缓冲区读取记录
        // 格式: [8字节长度][数据...]
        loop {
            // 尝试读取长度前缀
            if buffer.remaining() < 8 {
                if buffer.exhausted {
                    return Err(Error::Eof);
                }
                // 需要更多数据
                let reader = self.reader.read().await;
                let more_data = reader.read_raw(self.read_ahead_size).await?;
                if more_data.is_empty() {
                    buffer.exhausted = true;
                    return Err(Error::Eof);
                }
                buffer.data.extend_from_slice(&more_data);
                continue;
            }

            // 读取长度前缀
            let length_bytes = match buffer.read(8) {
                Some(b) => b,
                None => continue,
            };

            let length = u64::from_be_bytes([
                length_bytes[0],
                length_bytes[1],
                length_bytes[2],
                length_bytes[3],
                length_bytes[4],
                length_bytes[5],
                length_bytes[6],
                length_bytes[7],
            ]) as usize;

            // 验证长度合理性
            if length == 0 || length > MAX_RECORD_SIZE as usize {
                // 长度无效，回退一字节重试
                if buffer.pos > 0 {
                    buffer.pos -= 1;
                }
                continue;
            }

            // 尝试读取数据
            while buffer.remaining() < length {
                if buffer.exhausted {
                    return Err(Error::Eof);
                }
                let reader = self.reader.read().await;
                let more_data = reader.read_raw(self.read_ahead_size).await?;
                if more_data.is_empty() {
                    buffer.exhausted = true;
                    return Err(Error::Eof);
                }
                buffer.data.extend_from_slice(&more_data);
            }

            match buffer.read(length) {
                Some(data) => return Ok(data),
                None => continue,
            }
        }
    }

    /// 批量顺序读取
    pub async fn read_batch(&self, max_count: usize) -> Result<Vec<Vec<u8>>> {
        let mut records = Vec::with_capacity(max_count);

        for _ in 0..max_count {
            match self.read_next().await {
                Ok(data) => records.push(data),
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// 跳转到指定位置（清除预读缓冲）
    pub async fn seek(&self, segment_id: u64, offset: u64) {
        let mut buffer = self.read_ahead_buffer.write().await;
        buffer.clear();

        let reader = self.reader.read().await;
        reader.seek(segment_id, offset).await;
    }

    /// 跳转到开头
    pub async fn seek_to_start(&self) {
        let mut buffer = self.read_ahead_buffer.write().await;
        buffer.clear();

        let reader = self.reader.read().await;
        reader.seek_to_start().await;
    }

    /// 获取当前位置
    pub async fn position(&self) -> ReadPosition {
        let reader = self.reader.read().await;
        reader.position().await
    }

    /// 获取段信息
    pub async fn segments(&self) -> Vec<SegmentMeta> {
        let reader = self.reader.read().await;
        reader.segments().await
    }

    /// 获取段数量
    pub async fn segment_count(&self) -> usize {
        let reader = self.reader.read().await;
        reader.segment_count().await
    }

    /// 关闭读取协调器
    pub async fn close(&self) -> Result<()> {
        let reader = self.reader.read().await;
        reader.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LogReaderConfig, LogWriterConfig};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_coordinator_basic() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true);

        let writer = Arc::new(LogWriter::new(config).await.unwrap());
        let coordinator = WriteCoordinator::new(writer);

        let pos = coordinator.write(b"test data").await.unwrap();
        assert_eq!(pos.segment_id, 1);
        assert_eq!(pos.length, 9);

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_read_coordinator_basic() {
        let temp_dir = tempdir().unwrap();

        let writer_config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true);
        let writer = Arc::new(LogWriter::new(writer_config).await.unwrap());
        writer.write(b"hello").await.unwrap();
        writer.close().await.unwrap();

        let reader_config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = Arc::new(RwLock::new(LogReader::new(reader_config).await.unwrap()));
        let coordinator = ReadCoordinator::new(reader);

        coordinator.seek_to_start().await;
        let result = coordinator.read_next().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"hello");

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_read_write_round_trip() {
        let temp_dir = tempdir().unwrap();

        let writer_config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true);
        let writer = Arc::new(LogWriter::new(writer_config).await.unwrap());
        let write_coord = WriteCoordinator::new(writer);

        write_coord.write(b"record1").await.unwrap();
        write_coord.write(b"record2").await.unwrap();
        write_coord.write(b"record3").await.unwrap();
        write_coord.close().await.unwrap();

        let reader_config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = Arc::new(RwLock::new(LogReader::new(reader_config).await.unwrap()));
        let read_coord = ReadCoordinator::new(reader);

        read_coord.seek_to_start().await;

        // 读取所有记录直到 EOF
        let mut count = 0;
        while let Ok(data) = read_coord.read_next().await {
            count += 1;
            assert!(!data.is_empty());
        }

        // 至少能读到3条记录
        assert!(count >= 3);

        read_coord.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_write_small_data() {
        let temp_dir = tempdir().unwrap();

        let writer_config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true);
        let writer = Arc::new(LogWriter::new(writer_config).await.unwrap());
        let write_coord = WriteCoordinator::new(writer);

        // 小数据聚合写入
        let data_list: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        let positions = write_coord.write_batch(&data_list).await.unwrap();
        assert_eq!(positions.len(), 3);

        write_coord.close().await.unwrap();
    }
}
