//! Storage Layer - 底层存储抽象
//!
//! 这是 WAL 系统的最底层，提供统一的存储接口抽象。
//!
//! # 教学价值
//! - 学习 Rust trait 设计
//! - 学习依赖注入模式
//! - 学习错误处理最佳实践
//! - 学习文件 I/O 操作

pub mod checksum;
pub mod file_storage;
pub mod log_reader;
pub mod log_writer;
pub mod memory_storage;
pub mod segment_manager;
pub mod sync_strategy;

pub use checksum::{Crc32, crc32, verify_crc32};
pub use file_storage::FileStorage;
pub use log_reader::{LogReader, LogReaderConfig, ReadPosition};
pub use log_writer::{LogWriter, LogWriterConfig, WritePosition};
pub use memory_storage::MemoryStorage;
pub use segment_manager::{SegmentConfig, SegmentManager, SegmentMeta, SegmentStats};
pub use sync_strategy::{SyncMode, SyncStats, SyncStrategy};

use crate::prelude::*;
use async_trait::async_trait;
use std::path::Path;

/// 存储层统计信息
#[derive(Debug, Clone, Default)]
pub struct StorageStats {
    /// 当前文件大小（字节）
    pub size: u64,
    /// 总写入字节数
    pub bytes_written: u64,
    /// 总读取字节数
    pub bytes_read: u64,
    /// 写入操作次数
    pub write_ops: u64,
    /// 读取操作次数
    pub read_ops: u64,
}

/// 存储位置信息
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    /// 文件偏移量
    pub offset: u64,
    /// 数据长度
    pub length: u64,
}

impl Location {
    pub fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }
}

/// 存储接口抽象
///
/// 这是 WAL 系统最底层的抽象，所有的读写操作都通过这个接口进行。
///
/// # 设计原则
/// 1. **简单性**: 只提供必要的操作，避免过度设计
/// 2. **可靠性**: 所有操作都返回 Result，强制错误处理
/// 3. **可测试性**: 通过 trait 抽象，方便 mock 和测试
/// 4. **异步友好**: 使用 async_trait 支持异步实现
///
/// # 教学要点
/// - trait 定义了共享的行为
/// - 使用 async_trait 支持异步 trait
/// - `Send + Sync` 标记确保线程安全
#[async_trait]
pub trait Storage: Send + Sync {
    /// 读取指定位置的数据
    ///
    /// # 参数
    /// - `offset`: 文件偏移量
    /// - `length`: 读取长度
    ///
    /// # 返回
    /// 读取到的数据，如果超出文件末尾返回已有数据
    async fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>>;

    /// 写入数据到指定位置
    ///
    /// # 参数
    /// - `offset`: 文件偏移量
    /// - `data`: 要写入的数据
    ///
    /// # 注意
    /// 数据写入后可能还在缓冲区，需要调用 `sync()` 确保持久化
    async fn write(&self, offset: u64, data: &[u8]) -> Result<()>;

    /// 追加数据到文件末尾
    ///
    /// # 返回
    /// 返回写入的起始位置
    async fn append(&self, data: &[u8]) -> Result<u64>;

    /// 批量读取多个数据块
    ///
    /// # 参数
    /// - `locations`: 位置列表
    ///
    /// # 返回
    /// 按顺序返回读取的数据
    async fn read_batch(&self, locations: &[Location]) -> Result<Vec<Vec<u8>>>;

    /// 批量写入多个数据块
    ///
    /// # 参数
    /// - `offsets`: 偏移量列表
    /// - `data_list`: 数据列表
    ///
    /// # 注意
    /// 实现应该保证原子性：要么全部成功，要么全部失败
    async fn write_batch(&self, offsets: &[u64], data_list: &[&[u8]]) -> Result<()>;

    /// 同步数据到持久化存储
    ///
    /// 确保所有写入的数据都已经持久化到磁盘
    async fn sync(&self) -> Result<()>;

    /// 获取当前存储大小（字节）
    async fn size(&self) -> Result<u64>;

    /// 截断文件到指定大小
    ///
    /// # 参数
    /// - `length`: 新的文件大小
    async fn truncate(&self, length: u64) -> Result<()>;

    /// 获取存储统计信息
    async fn stats(&self) -> StorageStats;

    /// 关闭存储
    ///
    /// 释放资源，确保所有数据都已持久化
    async fn close(&self) -> Result<()>;
}

/// 创建文件存储的辅助函数
pub async fn create_file_storage<P: AsRef<Path> + Send + Sync>(path: P) -> Result<FileStorage> {
    FileStorage::new(path).await
}

/// 创建内存存储的辅助函数（用于测试）
pub fn create_memory_storage() -> MemoryStorage {
    MemoryStorage::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_location_new() {
        let loc = Location::new(100, 50);
        assert_eq!(loc.offset, 100);
        assert_eq!(loc.length, 50);
    }

    #[test]
    fn test_storage_stats_default() {
        let stats = StorageStats::default();
        assert_eq!(stats.size, 0);
        assert_eq!(stats.bytes_written, 0);
        assert_eq!(stats.bytes_read, 0);
    }
}
