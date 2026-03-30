//! 读取协调器
//!
//! 提供读取协调和预读缓冲优化。

use crate::prelude::*;
use crate::storage::{LogReader, ReadPosition, SegmentMeta};
use std::sync::Arc;
use tokio::sync::RwLock;

/// 读取协调器
///
/// 协调读取操作，提供：
/// - 预读缓冲优化
/// - 批量读取支持
/// - 位置管理
pub struct ReadCoordinator {
    reader: Arc<RwLock<LogReader>>,
    /// 预读缓冲区
    read_ahead_buffer: Arc<RwLock<ReadAheadBuffer>>,
    /// 预读大小
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
    /// 是否有残留的不完整数据（无法解析出完整记录）
    has_incomplete: bool,
}

impl ReadAheadBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            pos: 0,
            exhausted: false,
            has_incomplete: false,
        }
    }

    /// 清空缓冲区
    fn clear(&mut self) {
        self.data.clear();
        self.pos = 0;
        self.exhausted = false;
        self.has_incomplete = false;
    }

    /// 检查缓冲区是否有可用的完整数据
    /// 如果有残留的不完整数据，返回 false 以强制重新填充
    fn has_data(&self) -> bool {
        // 有不完整的残留数据时，返回 false 强制重新填充
        if self.has_incomplete {
            return false;
        }
        self.pos < self.data.len()
    }

    /// 从缓冲区读取数据
    fn read(&mut self) -> Option<Vec<u8>> {
        if !self.has_data() {
            return None;
        }

        // 读取 Magic (4 bytes)
        if self.pos + 4 > self.data.len() {
            return None;
        }
        let magic_bytes: [u8; 4] = self.data[self.pos..self.pos + 4].try_into().unwrap();
        let magic = u32::from_be_bytes(magic_bytes);

        // 验证 Magic
        if magic != crate::storage::format::RECORD_MAGIC {
            return None;
        }

        // 读取长度 (4 bytes)
        if self.pos + 8 > self.data.len() {
            return None;
        }
        let length_bytes: [u8; 4] = self.data[self.pos + 4..self.pos + 8].try_into().unwrap();
        let length = u32::from_be_bytes(length_bytes) as usize;

        // 验证数据完整性
        let record_size = 4 + 4 + 4 + length; // magic + length + crc + data
        if self.pos + record_size > self.data.len() {
            // 标记有残留不完整数据，下次 has_data() 将返回 false
            self.has_incomplete = true;
            return None;
        }

        // 提取数据（跳过 magic 4B + length 4B + crc 4B）
        let data_start = self.pos + 12;
        let data = self.data[data_start..data_start + length].to_vec();

        self.pos += record_size;
        Some(data)
    }

    /// 填充缓冲区
    fn fill(&mut self, data: Vec<u8>) {
        self.data = data;
        self.pos = 0;
        self.exhausted = false;
        self.has_incomplete = false;
    }
}

impl ReadCoordinator {
    /// 创建读取协调器
    pub fn new(reader: Arc<RwLock<LogReader>>) -> Self {
        Self {
            reader,
            read_ahead_buffer: Arc::new(RwLock::new(ReadAheadBuffer::new(64 * 1024))),
            read_ahead_size: 64 * 1024,
        }
    }

    /// 设置预读大小
    pub fn with_read_ahead(mut self, size: usize) -> Self {
        self.read_ahead_size = size;
        self.read_ahead_buffer = Arc::new(RwLock::new(ReadAheadBuffer::new(size)));
        self
    }

    /// 尝试从预读缓冲区读取
    async fn read_from_buffer(&self) -> Option<Vec<u8>> {
        let mut buffer = self.read_ahead_buffer.write().await;
        buffer.read()
    }

    /// 填充预读缓冲区
    async fn fill_buffer(&self) -> Result<()> {
        let mut buffer = self.read_ahead_buffer.write().await;

        // 只有当缓冲区完全为空时才填充
        // 如果缓冲区有残留数据但无法解析，说明是损坏或不完整记录，直接清空
        if buffer.has_data() {
            return Ok(());
        }

        // 从 LogReader 读取原始数据填充缓冲区
        let raw_data = {
            let reader = self.reader.read().await;
            reader.read_raw(self.read_ahead_size).await?
        };

        if raw_data.is_empty() {
            buffer.exhausted = true;
            return Err(Error::Eof);
        }

        buffer.fill(raw_data);
        Ok(())
    }

    /// 获取底层读取器
    pub async fn reader(&self) -> Arc<RwLock<LogReader>> {
        self.reader.clone()
    }

    /// 获取预读大小
    pub fn read_ahead_size(&self) -> usize {
        self.read_ahead_size
    }

    /// 读取下一条记录
    ///
    /// 优先从预读缓冲区读取，缓冲区耗尽时自动填充
    /// 格式：[4 字节 magic][4 字节长度][4 字节 CRC32][数据...]
    pub async fn read_next(&self) -> Result<Vec<u8>> {
        // 尝试从预读缓冲区读取
        if let Some(data) = self.read_from_buffer().await {
            return Ok(data);
        }

        // 缓冲区为空，填充缓冲区
        match self.fill_buffer().await {
            Ok(()) => {
                // 再次尝试从缓冲区读取
                if let Some(data) = self.read_from_buffer().await {
                    return Ok(data);
                }
                // 缓冲区填充后仍无数据或无法解析，说明到达末尾
                Err(Error::Eof)
            }
            Err(Error::Eof) => Err(Error::Eof),
            Err(e) => Err(e),
        }
    }

    /// 批量顺序读取
    ///
    /// 利用预读缓冲区优化批量读取性能
    pub async fn read_batch(&self, max_count: usize) -> Result<Vec<Vec<u8>>> {
        let mut records = Vec::with_capacity(max_count);

        // 预先填充缓冲区
        let _ = self.fill_buffer().await;

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
        {
            let mut buffer = self.read_ahead_buffer.write().await;
            buffer.clear();
        }

        {
            let reader = self.reader.read().await;
            reader.seek_to_start().await;
        }

        // 预填充缓冲区
        let _ = self.fill_buffer().await;
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
    use crate::storage::{LogReaderConfig, LogWriter, LogWriterConfig, SegmentConfig};
    use crate::wal::{CommitConfig, CommitCoordinator, RotationConfig, SegmentCoordinator};
    use tempfile::tempdir;

    /// 辅助函数：创建测试用的写入器和写入数据
    #[tokio::test]
    async fn test_read_coordinator_basic() {
        let temp_dir = tempdir().unwrap();

        // 使用 CommitCoordinator 写入数据
        let rotation_config = RotationConfig::new();
        let segment_config = SegmentConfig::new(temp_dir.path());
        let segment_coordinator = Arc::new(
            SegmentCoordinator::new(rotation_config, segment_config)
                .await
                .unwrap(),
        );

        let commit_config = CommitConfig::default();
        let commit_coordinator = Arc::new(
            CommitCoordinator::new(commit_config, segment_coordinator)
                .await
                .unwrap(),
        );
        commit_coordinator.start();

        // 注册 writer 并写入数据
        let writer = commit_coordinator.register_writer(None).await.unwrap();
        writer.write(b"hello").await.unwrap();
        writer.close().await.unwrap();

        commit_coordinator.shutdown().await;

        // 使用 ReadCoordinator 读取数据
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

        // 使用 CommitCoordinator 写入数据
        let rotation_config = RotationConfig::new();
        let segment_config = SegmentConfig::new(temp_dir.path());
        let segment_coordinator = Arc::new(
            SegmentCoordinator::new(rotation_config, segment_config)
                .await
                .unwrap(),
        );

        let commit_config = CommitConfig::default();
        let commit_coordinator = Arc::new(
            CommitCoordinator::new(commit_config, segment_coordinator)
                .await
                .unwrap(),
        );
        commit_coordinator.start();

        // 注册 writer 并写入多条数据
        let writer = commit_coordinator.register_writer(None).await.unwrap();
        writer.write(b"record1").await.unwrap();
        writer.write(b"record2").await.unwrap();
        writer.write(b"record3").await.unwrap();
        writer.close().await.unwrap();

        commit_coordinator.shutdown().await;

        // 使用 ReadCoordinator 读取数据
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
}
