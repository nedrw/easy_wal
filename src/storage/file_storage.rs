//! 文件存储实现（生产环境使用）
//!
//! 提供基于文件的持久化存储实现。
//!
//! # 教学价值
//! - 学习异步文件 I/O 操作
//! - 学习文件同步和持久化机制
//! - 学习错误处理和资源管理
//! - 学习线程安全的文件访问

use super::{Location, Storage, StorageStats};
use crate::prelude::*;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::sync::RwLock;

/// 文件存储实现
///
/// 使用文件系统进行数据持久化，适合生产环境。
///
/// # 线程安全
/// 使用 `RwLock` 保护文件句柄，支持并发读取。
///
/// # 性能特点
/// - 读取：需要文件 I/O，性能受磁盘速度影响
/// - 写入：写入到操作系统缓冲区，sync 后才真正持久化
/// - 持久性：调用 sync() 后确保数据写入磁盘
///
/// # 错误处理
/// 所有可能失败的操作都返回 Result，需要调用者处理错误。
pub struct FileStorage {
    /// 文件路径
    path: PathBuf,
    /// 文件句柄，使用 RwLock 保证线程安全
    file: RwLock<File>,
    /// 统计信息
    stats: RwLock<StorageStats>,
}

impl FileStorage {
    /// 创建新的文件存储
    ///
    /// # 参数
    /// - `path`: 文件路径
    ///
    /// # 返回
    /// 成功返回 FileStorage 实例，失败返回错误
    ///
    /// # 注意
    /// 如果文件不存在，会自动创建；如果文件已存在，会打开现有文件。
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 确保父目录存在
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| Error::Generic(format!("Failed to create directory: {}", e)))?;
            }
        }

        // 打开或创建文件
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .await
            .map_err(|e| Error::Generic(format!("Failed to open file {:?}: {}", path, e)))?;

        // 获取文件大小
        let metadata = file
            .metadata()
            .await
            .map_err(|e| Error::Generic(format!("Failed to get metadata: {}", e)))?;
        let size = metadata.len();

        Ok(Self {
            path,
            file: RwLock::new(file),
            stats: RwLock::new(StorageStats {
                size,
                ..Default::default()
            }),
        })
    }

    /// 获取文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 内部读取方法，不更新统计信息
    async fn read_internal(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        let mut file = self.file.write().await;

        // 定位到指定位置
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| Error::Generic(format!("Seek failed: {}", e)))?;

        // 读取数据
        let mut buffer = vec![0u8; length as usize];
        let bytes_read = file
            .read(&mut buffer)
            .await
            .map_err(|e| Error::Generic(format!("Read failed: {}", e)))?;

        // 如果读取的字节数少于请求的长度，截断缓冲区
        buffer.truncate(bytes_read);

        Ok(buffer)
    }

    /// 内部写入方法，不更新统计信息
    async fn write_internal(&self, offset: u64, data: &[u8]) -> Result<()> {
        let mut file = self.file.write().await;

        // 定位到指定位置
        file.seek(std::io::SeekFrom::Start(offset))
            .await
            .map_err(|e| Error::Generic(format!("Seek failed: {}", e)))?;

        // 写入数据
        file.write_all(data)
            .await
            .map_err(|e| Error::Generic(format!("Write failed: {}", e)))?;

        Ok(())
    }
}

#[async_trait]
impl Storage for FileStorage {
    /// 读取指定位置的数据
    ///
    /// # 实现
    /// 使用 seek + read 进行文件读取
    async fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        let result = self.read_internal(offset, length).await?;

        // 更新统计信息
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
    /// 使用 seek + write 进行文件写入
    async fn write(&self, offset: u64, data: &[u8]) -> Result<()> {
        self.write_internal(offset, data).await?;

        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.bytes_written += data.len() as u64;
            stats.write_ops += 1;

            // 更新文件大小
            let current_size = self.size().await?;
            if current_size > stats.size {
                stats.size = current_size;
            }
        }

        Ok(())
    }

    /// 追加数据到文件末尾
    ///
    /// # 实现
    /// 先获取文件大小作为偏移量，然后写入数据
    async fn append(&self, data: &[u8]) -> Result<u64> {
        let mut file = self.file.write().await;

        // 定位到文件末尾并获取当前位置（即偏移量）
        file.seek(std::io::SeekFrom::End(0))
            .await
            .map_err(|e| Error::Generic(format!("Seek failed: {}", e)))?;
        let offset = file
            .stream_position()
            .await
            .map_err(|e| Error::Generic(format!("Failed to get position: {}", e)))?;

        // 写入数据
        file.write_all(data)
            .await
            .map_err(|e| Error::Generic(format!("Write failed: {}", e)))?;

        // 更新统计信息
        drop(file);
        {
            let mut stats = self.stats.write().await;
            stats.bytes_written += data.len() as u64;
            stats.write_ops += 1;
            stats.size = offset + data.len() as u64;
        }

        Ok(offset)
    }

    /// 批量读取多个数据块
    ///
    /// # 实现
    /// 顺序读取每个位置的数据
    ///
    /// # 性能优化
    /// 未来可以使用预读优化
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
    ///
    /// # 性能优化
    /// 使用单个 write 系统调用批量写入
    async fn write_batch(&self, offsets: &[u64], data_list: &[&[u8]]) -> Result<()> {
        if offsets.len() != data_list.len() {
            return Err(Error::Generic(
                "offsets and data_list must have the same length".to_string(),
            ));
        }

        // 计算总大小，一次性分配
        let total_size: usize = data_list.iter().map(|d| d.len()).sum();

        // 按偏移量排序，避免频繁 seek
        let mut indexed_data: Vec<_> = offsets.iter().zip(data_list.iter()).enumerate().collect();
        indexed_data.sort_by_key(|&(_, (&offset, _))| offset);

        // 写入数据
        for (_, (&offset, data)) in indexed_data {
            self.write_internal(offset, data).await?;
        }

        // 更新统计信息
        {
            let mut stats = self.stats.write().await;
            stats.bytes_written += total_size as u64;
            stats.write_ops += 1;

            // 更新文件大小
            let current_size = self.size().await?;
            if current_size > stats.size {
                stats.size = current_size;
            }
        }

        Ok(())
    }

    /// 同步数据到磁盘
    ///
    /// # 实现
    /// 调用 fsync 确保数据持久化
    ///
    /// # 性能影响
    /// 这是一个昂贵的操作，会阻塞直到数据写入磁盘
    async fn sync(&self) -> Result<()> {
        let file = self.file.read().await;
        file.sync_all()
            .await
            .map_err(|e| Error::Generic(format!("Sync failed: {}", e)))?;
        Ok(())
    }

    /// 获取文件大小
    async fn size(&self) -> Result<u64> {
        let file = self.file.read().await;
        let metadata = file
            .metadata()
            .await
            .map_err(|e| Error::Generic(format!("Failed to get metadata: {}", e)))?;
        Ok(metadata.len())
    }

    /// 截断文件到指定大小
    ///
    /// # 实现
    /// 使用 set_len 方法截断文件
    async fn truncate(&self, length: u64) -> Result<()> {
        let file = self.file.write().await;
        file.set_len(length)
            .await
            .map_err(|e| Error::Generic(format!("Truncate failed: {}", e)))?;

        // 更新统计信息
        drop(file);
        {
            let mut stats = self.stats.write().await;
            stats.size = length;
        }

        Ok(())
    }

    /// 获取存储统计信息
    async fn stats(&self) -> StorageStats {
        self.stats.read().await.clone()
    }

    /// 关闭文件
    ///
    /// # 实现
    /// 先同步数据，然后释放文件句柄
    async fn close(&self) -> Result<()> {
        // 确保数据已同步
        self.sync().await?;

        // 文件会在 FileStorage 被 drop 时自动关闭
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_basic_operations() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.wal");

        let storage = FileStorage::new(&file_path).await.unwrap();

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
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.wal");

        let storage = FileStorage::new(&file_path).await.unwrap();

        // 写入数据
        storage.write(0, b"hello").await.unwrap();
        storage.write(5, b" world").await.unwrap();

        // 读取数据
        let data = storage.read(0, 11).await.unwrap();
        assert_eq!(data, b"hello world");
    }

    #[tokio::test]
    async fn test_sync() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.wal");

        let storage = FileStorage::new(&file_path).await.unwrap();

        // 写入数据
        storage.append(b"hello").await.unwrap();

        // 同步到磁盘
        storage.sync().await.unwrap();

        // 验证文件存在
        assert!(file_path.exists());
    }

    #[tokio::test]
    async fn test_stats() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.wal");

        let storage = FileStorage::new(&file_path).await.unwrap();

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
    async fn test_persistence() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.wal");

        // 第一次写入
        {
            let storage = FileStorage::new(&file_path).await.unwrap();
            storage.append(b"hello world").await.unwrap();
            storage.sync().await.unwrap();
        }

        // 重新打开验证数据持久化
        {
            let storage = FileStorage::new(&file_path).await.unwrap();
            let data = storage.read(0, 11).await.unwrap();
            assert_eq!(data, b"hello world");
        }
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.wal");

        let storage = FileStorage::new(&file_path).await.unwrap();

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
    async fn test_out_of_bounds_read() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.wal");

        let storage = FileStorage::new(&file_path).await.unwrap();

        // 在空文件上读取
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

        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("test.wal");

        let storage = Arc::new(FileStorage::new(&file_path).await.unwrap());
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

        // 同步数据
        storage.sync().await.unwrap();

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

    #[tokio::test]
    async fn test_auto_create_directory() {
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("nested").join("dir").join("test.wal");

        let storage = FileStorage::new(&file_path).await.unwrap();
        storage.append(b"test").await.unwrap();
        storage.sync().await.unwrap();

        assert!(file_path.exists());
    }
}
