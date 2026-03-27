//! WAL 管理器 - 统一的读写接口
//!
//! # 教学价值
//! - 学习 API 设计
//! - 学习 Builder 模式
//! - 学习组件集成

use super::{LogReader, LogReaderConfig, LogWriter, LogWriterConfig, ReadPosition, WritePosition};
use crate::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// WAL 配置
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// 目录路径
    pub dir: std::path::PathBuf,
    /// 最大段大小
    pub max_segment_size: u64,
    /// 写入后同步
    pub sync_on_write: bool,
    /// 批量大小
    pub batch_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: std::path::PathBuf::from("wal_data"),
            max_segment_size: 64 * 1024 * 1024, // 64MB
            sync_on_write: false,
            batch_size: 100,
        }
    }
}

impl WalConfig {
    pub fn with_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.dir = dir.as_ref().to_path_buf();
        self
    }

    pub fn with_max_segment_size(mut self, size: u64) -> Self {
        self.max_segment_size = size;
        self
    }

    pub fn with_sync_on_write(mut self, sync: bool) -> Self {
        self.sync_on_write = sync;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

/// WAL 记录
#[derive(Debug, Clone)]
pub struct Record {
    /// 数据
    pub data: Vec<u8>,
    /// 写入位置
    pub position: WritePosition,
}

/// WAL 管理器
///
/// 提供统一的读写接口，内部协调 LogWriter 和 LogReader。
pub struct WalManager {
    writer: Arc<LogWriter>,
    reader: Arc<RwLock<LogReader>>,
    #[allow(dead_code)]
    config: WalConfig,
}

impl WalManager {
    /// 创建 WAL 管理器（同步版本）
    pub async fn new(config: WalConfig) -> Result<Self> {
        // 创建目录
        tokio::fs::create_dir_all(&config.dir).await?;

        // 创建写入器
        let writer_config = LogWriterConfig::default()
            .with_dir(&config.dir)
            .with_max_segment_size(config.max_segment_size)
            .with_sync_on_write(config.sync_on_write);
        let writer = Arc::new(LogWriter::new(writer_config).await?);

        // 创建读取器
        let reader_config = LogReaderConfig::default()
            .with_dir(&config.dir)
            .with_batch_size(config.batch_size);
        let reader = Arc::new(RwLock::new(LogReader::new(reader_config).await?));

        Ok(Self {
            writer,
            reader,
            config,
        })
    }

    /// 写入数据
    ///
    /// 使用 LogWriter 内置的长度前缀格式：8 字节长度 + 数据
    pub async fn write(&self, data: &[u8]) -> Result<WritePosition> {
        self.writer.write(data).await
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

    /// 读取下一条记录
    pub async fn read(&self) -> Result<Record> {
        let reader = self.reader.read().await;

        // read_next 已经读取了长度前缀，返回的是实际数据
        let data = reader.read_next().await?;
        let length = data.len() as u64;

        // 获取读取后的位置
        let pos = reader.position().await;

        Ok(Record {
            data,
            position: WritePosition {
                segment_id: pos.segment_id,
                offset: pos.offset,
                length,
            },
        })
    }

    /// 批量读取
    pub async fn read_batch(&self, max_count: usize) -> Result<Vec<Record>> {
        let _reader = self.reader.read().await;
        let mut records = Vec::with_capacity(max_count);

        for _ in 0..max_count {
            match self.read().await {
                Ok(record) => records.push(record),
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// 跳到指定位置
    pub async fn seek(&self, segment_id: u64, offset: u64) {
        let reader = self.reader.read().await;
        reader.seek(segment_id, offset).await;
    }

    /// 跳到开头
    pub async fn seek_to_start(&self) {
        let reader = self.reader.read().await;
        reader.seek_to_start().await;
    }

    /// 获取当前位置
    pub async fn position(&self) -> ReadPosition {
        let reader = self.reader.read().await;
        reader.position().await
    }

    /// 获取所有段信息
    pub async fn segments(&self) -> Vec<super::SegmentMeta> {
        let reader = self.reader.read().await;
        reader.segments().await
    }

    /// 同步数据
    pub async fn sync(&self) -> Result<()> {
        self.writer.sync().await
    }

    /// 关闭 WAL
    pub async fn close(&self) -> Result<()> {
        self.writer.close().await?;
        let reader = self.reader.read().await;
        reader.close().await
    }
}

/// WAL 构建器
pub struct WalBuilder {
    config: WalConfig,
}

impl WalBuilder {
    pub fn new() -> Self {
        Self {
            config: WalConfig::default(),
        }
    }

    pub fn with_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.config.dir = dir.as_ref().to_path_buf();
        self
    }

    pub fn with_max_segment_size(mut self, size: u64) -> Self {
        self.config.max_segment_size = size;
        self
    }

    pub fn with_sync_on_write(mut self, sync: bool) -> Self {
        self.config.sync_on_write = sync;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    pub async fn build(&self) -> Result<WalManager> {
        WalManager::new(self.config.clone()).await
    }
}

impl Default for WalBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_write_read() {
        let temp_dir = tempdir().unwrap();

        let wal = WalBuilder::new()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true)
            .build()
            .await
            .unwrap();

        // 写入数据
        let pos = wal.write(b"hello world").await.unwrap();
        assert_eq!(pos.segment_id, 1);

        // 跳到开头读取
        wal.seek_to_start().await;

        // 读取数据
        let record = wal.read().await.unwrap();
        assert_eq!(record.data, b"hello world");

        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_batch_write_read() {
        let temp_dir = tempdir().unwrap();

        let wal = WalBuilder::new()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true)
            .build()
            .await
            .unwrap();

        // 批量写入
        let data_list: Vec<&[u8]> = vec![b"a", b"bb", b"ccc", b"dddd"];
        let positions = wal.write_batch(&data_list).await.unwrap();
        assert_eq!(positions.len(), 4);

        // 跳到开头批量读取
        wal.seek_to_start().await;
        let records = wal.read_batch(10).await.unwrap();

        assert!(records.len() >= 4);
        assert_eq!(records[0].data, b"a");
        assert_eq!(records[1].data, b"bb");

        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_seek() {
        let temp_dir = tempdir().unwrap();

        let wal = WalBuilder::new()
            .with_dir(temp_dir.path())
            .with_sync_on_write(true)
            .build()
            .await
            .unwrap();

        wal.write(b"first").await.unwrap();
        wal.write(b"second").await.unwrap();

        // 跳到位置 0 读取第一条
        wal.seek(1, 0).await;
        let record = wal.read().await.unwrap();
        assert_eq!(record.data, b"first");

        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_segments() {
        let temp_dir = tempdir().unwrap();

        let wal = WalBuilder::new()
            .with_dir(temp_dir.path())
            .with_max_segment_size(10)
            .with_sync_on_write(true)
            .build()
            .await
            .unwrap();

        // 写入数据触发轮转
        wal.write(b"12345678901").await.unwrap();

        let segments = wal.segments().await;
        assert!(segments.len() >= 1);

        wal.close().await.unwrap();
    }
}
