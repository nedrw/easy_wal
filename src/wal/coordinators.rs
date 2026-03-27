//! WAL 协调器 - 读写协调组件
//!
//! # 教学价值
//! - 学习协调器模式
//! - 学习读写分离
//! - 学习资源调度

use crate::prelude::*;
use crate::storage::{LogReader, LogWriter};
use std::sync::Arc;
use tokio::sync::RwLock;

/// 写入协调器
///
/// 协调写入操作，提供：
/// - 写入请求队列
/// - 批量写入优化
/// - 与 RecoveryManager 协作
pub struct WriteCoordinator {
    writer: Arc<LogWriter>,
    /// 是否启用批量写入
    batch_enabled: bool,
    /// 批量写入阈值
    batch_threshold: usize,
}

impl WriteCoordinator {
    /// 创建写入协调器
    pub fn new(writer: Arc<LogWriter>) -> Self {
        Self {
            writer,
            batch_enabled: false,
            batch_threshold: 100,
        }
    }

    /// 启用批量写入
    pub fn with_batch(mut self, threshold: usize) -> Self {
        self.batch_enabled = true;
        self.batch_threshold = threshold;
        self
    }

    /// 获取底层写入器
    pub fn writer(&self) -> Arc<LogWriter> {
        self.writer.clone()
    }

    /// 写入单条数据
    pub async fn write(&self, data: &[u8]) -> Result<super::WritePosition> {
        self.writer.write(data).await
    }

    /// 批量写入
    pub async fn write_batch(&self, data_list: &[&[u8]]) -> Result<Vec<super::WritePosition>> {
        self.writer.write_batch(data_list).await
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

/// 读取协调器
///
/// 协调读取操作，提供：
/// - 读取位置管理
/// - 并发读取控制
/// - 与 RecoveryManager 协作
pub struct ReadCoordinator {
    reader: Arc<RwLock<LogReader>>,
    /// 预读缓冲区大小
    read_ahead_size: usize,
}

impl ReadCoordinator {
    /// 创建读取协调器
    pub fn new(reader: Arc<RwLock<LogReader>>) -> Self {
        Self {
            reader,
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

    /// 读取下一条记录
    pub async fn read_next(&self) -> Result<Vec<u8>> {
        let reader = self.reader.read().await;
        reader.read_next().await
    }

    /// 批量顺序读取
    pub async fn read_batch(&self, max_count: usize) -> Result<Vec<Vec<u8>>> {
        let reader = self.reader.read().await;
        reader.read_batch(max_count).await
    }

    /// 跳转到指定位置
    pub async fn seek(&self, segment_id: u64, offset: u64) {
        let reader = self.reader.read().await;
        reader.seek(segment_id, offset).await;
    }

    /// 跳转到开头
    pub async fn seek_to_start(&self) {
        let reader = self.reader.read().await;
        reader.seek_to_start().await;
    }

    /// 获取当前位置
    pub async fn position(&self) -> super::ReadPosition {
        let reader = self.reader.read().await;
        reader.position().await
    }

    /// 获取段信息
    pub async fn segments(&self) -> Vec<super::SegmentMeta> {
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
    async fn test_write_coordinator() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true);

        let writer = Arc::new(LogWriter::new(config).await.unwrap());
        let coordinator = WriteCoordinator::new(writer);

        let pos = coordinator.write(b"test data").await.unwrap();
        assert_eq!(pos.segment_id, 1);

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_read_coordinator() {
        let temp_dir = tempdir().unwrap();

        // 先写入一些数据
        let writer_config = LogWriterConfig::default()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true);
        let writer = Arc::new(LogWriter::new(writer_config).await.unwrap());
        writer.write(b"hello").await.unwrap();
        writer.close().await.unwrap();

        // 使用读取协调器
        let reader_config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = Arc::new(RwLock::new(LogReader::new(reader_config).await.unwrap()));
        let coordinator = ReadCoordinator::new(reader);

        coordinator.seek_to_start().await;
        let data = coordinator.read_next().await.unwrap();
        assert_eq!(data, b"hello");

        coordinator.close().await.unwrap();
    }
}
