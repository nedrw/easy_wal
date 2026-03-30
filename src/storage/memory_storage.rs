//! 内存存储实现（用于测试）
//!
//! 提供一个基于内存的存储实现，主要用于单元测试和开发环境。
//!
//! # 教学价值
//! - 学习如何实现 trait
//! - 学习线程安全的数据结构设计
//! - 学习 RwLock 的使用场景

use super::{Location, Storage, StorageStats};
use crate::prelude::*;
use async_trait::async_trait;
use tokio::sync::RwLock;

/// 内存存储实现
///
/// 使用 `Vec<u8>` 存储所有数据，适合测试和开发环境。
///
/// # 线程安全
/// 使用 `RwLock` 保证线程安全，支持并发读取。
///
/// # 性能特点
/// - 读取：O(1) 时间复杂度
/// - 写入：O(1) 时间复杂度（追加），O(n) 时间复杂度（随机写入）
/// - 内存占用：与数据量成正比
pub struct MemoryStorage {
    /// 数据存储，使用 RwLock 保证线程安全
    data: RwLock<Vec<u8>>,
    /// 统计信息
    stats: RwLock<StorageStats>,
}

impl MemoryStorage {
    /// 创建新的内存存储
    pub fn new() -> Self {
        Self {
            data: RwLock::new(Vec::new()),
            stats: RwLock::new(StorageStats::default()),
        }
    }

    /// 创建带有初始容量的内存存储
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: RwLock::new(Vec::with_capacity(capacity)),
            stats: RwLock::new(StorageStats::default()),
        }
    }

    /// 获取内部数据的快照（用于测试）
    pub async fn snapshot(&self) -> Vec<u8> {
        self.data.read().await.clone()
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    /// 读取指定位置的数据
    ///
    /// # 实现
    /// 直接从 Vec 中读取，超出末尾返回已有数据
    async fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        // 读取操作作用域：完成后自动释放读锁
        let result = {
            let data = self.data.read().await;
            let offset = offset as usize;
            let length = length as usize;

            // 边界检查
            if offset >= data.len() {
                return Ok(Vec::new());
            }

            let end = std::cmp::min(offset + length, data.len());
            data[offset..end].to_vec()
        }; // data 在此自动释放

        // 更新统计信息（数据锁已释放）
        {
            let mut stats = self.stats.write().await;
            stats.bytes_read += result.len() as u64;
            stats.read_ops += 1;
        }

        Ok(result)
    }

    /// 写入数据到指定位置
    ///
    /// # 实现
    /// 如果超出当前大小，会自动扩展
    async fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        // 写入操作作用域：完成后自动释放写锁
        let (bytes_written, new_size) = {
            let mut storage = self.data.write().await;
            let offset = offset as usize;

            // 如果需要，扩展存储空间
            let required_len = offset + data.len();
            if storage.len() < required_len {
                storage.resize(required_len, 0);
            }

            // 写入数据
            storage[offset..offset + data.len()].copy_from_slice(data);

            (data.len() as u64, storage.len() as u64)
        }; // storage 在此自动释放

        // 更新统计信息（数据锁已释放）
        {
            let mut stats = self.stats.write().await;
            stats.bytes_written += bytes_written;
            stats.write_ops += 1;
            stats.size = new_size;
        }

        Ok(())
    }

    /// 追加数据到末尾
    ///
    /// # 实现
    /// 返回追加前的长度作为起始位置
    async fn append(&self, data: &[u8]) -> Result<u64> {
        // 追加操作作用域：完成后自动释放写锁
        let (offset, bytes_written, new_size) = {
            let mut storage = self.data.write().await;
            let offset = storage.len() as u64;
            storage.extend_from_slice(data);
            (offset, data.len() as u64, storage.len() as u64)
        }; // storage 在此自动释放

        // 更新统计信息（数据锁已释放）
        {
            let mut stats = self.stats.write().await;
            stats.bytes_written += bytes_written;
            stats.write_ops += 1;
            stats.size = new_size;
        }

        Ok(offset)
    }

    /// 批量追加数据到末尾
    ///
    /// # 实现
    /// 一次性获取当前末尾位置，然后批量追加所有数据
    /// 保证原子性：要么全部成功，要么全部失败
    async fn append_batch(&self, data_list: &[&[u8]]) -> Result<Vec<u64>> {
        if data_list.is_empty() {
            return Ok(Vec::new());
        }

        // 批量追加操作作用域：完成后自动释放写锁
        let (offsets, bytes_written, new_size) = {
            let mut storage = self.data.write().await;
            let start_offset = storage.len() as u64;

            // 预先计算每条数据的起始位置
            let mut offsets = Vec::with_capacity(data_list.len());
            let mut current_offset = start_offset;
            for data in data_list {
                offsets.push(current_offset);
                current_offset += data.len() as u64;
            }

            // 批量追加所有数据
            for data in data_list {
                storage.extend_from_slice(data);
            }

            let bytes_written: u64 = data_list.iter().map(|d| d.len() as u64).sum();
            (offsets, bytes_written, storage.len() as u64)
        }; // storage 在此自动释放

        // 更新统计信息（数据锁已释放）
        {
            let mut stats = self.stats.write().await;
            stats.bytes_written += bytes_written;
            stats.write_ops += 1;
            stats.size = new_size;
        }

        Ok(offsets)
    }

    /// 批量读取多个数据块
    ///
    /// # 实现
    /// 顺序读取每个位置的数据
    async fn read_batch(&self, locations: &[Location]) -> Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(locations.len());

        for loc in locations {
            let data = self.read(loc.offset, loc.length).await?;
            results.push(data);
        }

        Ok(results)
    }

    /// 批量写入多个数据块
    ///
    /// # 实现
    /// 顺序写入每个数据块
    async fn write_batch(&self, offsets: &[u64], data_list: &[&[u8]]) -> Result<()> {
        if offsets.len() != data_list.len() {
            return Err(Error::Generic(
                "offsets and data_list must have the same length".to_string(),
            ));
        }

        for (offset, data) in offsets.iter().zip(data_list.iter()) {
            self.write(*offset, data).await?;
        }

        Ok(())
    }

    /// 同步数据（内存存储无需同步）
    ///
    /// # 实现
    /// 空操作，因为数据已经在内存中
    async fn sync(&self) -> Result<()> {
        // 内存存储不需要同步，数据已经在"持久化"状态
        Ok(())
    }

    /// 获取当前存储大小
    async fn size(&self) -> Result<u64> {
        Ok(self.data.read().await.len() as u64)
    }

    /// 截断文件到指定大小
    ///
    /// # 实现
    /// 直接截断内部 Vec
    async fn truncate(&self, length: u64) -> Result<()> {
        // 截断操作作用域：完成后自动释放写锁
        let new_size = {
            let mut storage = self.data.write().await;
            storage.truncate(length as usize);
            storage.len() as u64
        }; // storage 在此自动释放

        // 更新统计信息（数据锁已释放）
        {
            let mut stats = self.stats.write().await;
            stats.size = new_size;
        }

        Ok(())
    }

    /// 获取存储统计信息
    async fn stats(&self) -> StorageStats {
        self.stats.read().await.clone()
    }

    /// 关闭存储（内存存储无需关闭）
    async fn close(&self) -> Result<()> {
        // 内存存储不需要特殊关闭逻辑
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_operations() {
        let storage = MemoryStorage::new();

        // 测试追加
        let offset = storage.append(b"hello").await.unwrap();
        assert_eq!(offset, 0);

        let offset = storage.append(b" world").await.unwrap();
        assert_eq!(offset, 5);

        // 测试读取
        let data = storage.read(0, 11).await.unwrap();
        assert_eq!(data, b"hello world");

        // 测试大小
        let size = storage.size().await.unwrap();
        assert_eq!(size, 11);
    }

    #[tokio::test]
    async fn test_write_and_read() {
        let storage = MemoryStorage::new();

        // 写入数据
        storage.write(0, b"hello").await.unwrap();
        storage.write(5, b" world").await.unwrap();

        // 读取数据
        let data = storage.read(0, 11).await.unwrap();
        assert_eq!(data, b"hello world");
    }

    #[tokio::test]
    async fn test_auto_expand() {
        let storage = MemoryStorage::new();

        // 在超出当前位置的地方写入
        storage.write(100, b"test").await.unwrap();

        // 验证自动扩展
        let size = storage.size().await.unwrap();
        assert_eq!(size, 104);

        // 验证中间数据被填充为 0
        let data = storage.read(0, 104).await.unwrap();
        assert_eq!(&data[0..100], &[0; 100]);
        assert_eq!(&data[100..104], b"test");
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let storage = MemoryStorage::new();

        // 批量写入
        let offsets = [0u64, 10, 20];
        let data_list: Vec<&[u8]> = vec![b"aaa", b"bbb", b"ccc"];
        storage.write_batch(&offsets, &data_list).await.unwrap();

        // 批量读取
        let locations = vec![
            Location::new(0, 3),
            Location::new(10, 3),
            Location::new(20, 3),
        ];
        let results = storage.read_batch(&locations).await.unwrap();

        assert_eq!(results[0], b"aaa");
        assert_eq!(results[1], b"bbb");
        assert_eq!(results[2], b"ccc");
    }

    #[tokio::test]
    async fn test_truncate() {
        let storage = MemoryStorage::new();

        // 写入数据
        storage.append(b"hello world").await.unwrap();
        assert_eq!(storage.size().await.unwrap(), 11);

        // 截断
        storage.truncate(5).await.unwrap();
        assert_eq!(storage.size().await.unwrap(), 5);

        // 验证数据
        let data = storage.read(0, 10).await.unwrap();
        assert_eq!(data, b"hello");
    }

    #[tokio::test]
    async fn test_stats() {
        let storage = MemoryStorage::new();

        // 执行一些操作
        storage.append(b"hello").await.unwrap();
        storage.append(b" world").await.unwrap();
        storage.read(0, 5).await.unwrap();

        // 检查统计信息
        let stats = storage.stats().await;
        assert_eq!(stats.bytes_written, 11);
        assert_eq!(stats.bytes_read, 5);
        assert_eq!(stats.write_ops, 2);
        assert_eq!(stats.read_ops, 1);
        assert_eq!(stats.size, 11);
    }

    #[tokio::test]
    async fn test_out_of_bounds_read() {
        let storage = MemoryStorage::new();

        // 在空存储上读取
        let data = storage.read(0, 10).await.unwrap();
        assert!(data.is_empty());

        // 写入一些数据
        storage.append(b"hello").await.unwrap();

        // 在末尾之后读取
        let data = storage.read(10, 5).await.unwrap();
        assert!(data.is_empty());

        // 部分超出
        let data = storage.read(3, 10).await.unwrap();
        assert_eq!(data, b"lo");
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        use std::sync::Arc;
        use tokio::task;

        let storage = Arc::new(MemoryStorage::new());
        let mut handles = vec![];

        // 并发写入
        for i in 0..10 {
            let s = storage.clone();
            let handle = task::spawn(async move {
                let data = format!("data_{}", i);
                s.append(data.as_bytes()).await.unwrap()
            });
            handles.push(handle);
        }

        // 等待所有写入完成
        let mut offsets = vec![];
        for handle in handles {
            offsets.push(handle.await.unwrap());
        }

        // 验证所有数据都写入了
        let size = storage.size().await.unwrap();
        assert!(size > 0);

        // 并发读取
        let mut handles = vec![];
        for offset in offsets {
            let s = storage.clone();
            let handle = task::spawn(async move { s.read(offset, 5).await.unwrap() });
            handles.push(handle);
        }

        // 等待所有读取完成
        for handle in handles {
            let _ = handle.await.unwrap();
        }
    }
}
