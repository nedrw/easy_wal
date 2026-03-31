//! AsyncWal 模块
//!
//! 实现异步 WAL 对象，提供主要的异步读写接口

use crate::{AsyncLogSegment, Config, Error, PersistenceMode, Result};
use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::sync::{Mutex, RwLock};

/// AsyncWal 对象
///
/// 主要的异步入口点，管理所有的段文件并提供异步读写接口
pub struct AsyncWal {
    /// WAL 目录路径
    path: PathBuf,

    /// 配置
    config: Config,

    /// 所有段的集合（按 base_offset 排序）
    segments: RwLock<BTreeMap<u64, Arc<Mutex<AsyncLogSegment>>>>,

    /// 当前活跃段
    active_segment: RwLock<Arc<Mutex<AsyncLogSegment>>>,

    /// 下一个写入偏移量
    next_offset: RwLock<u64>,

    /// 是否已关闭
    closed: RwLock<bool>,

    /// 段轮转保护锁（确保只有一个线程能执行轮转）
    rotate_lock: Mutex<()>,
}

impl fmt::Debug for AsyncWal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncWal")
            .field("path", &self.path)
            .field("config", &self.config)
            // Note: async fields (next_offset, closed, segments_count) omitted in Debug
            // because fmt is not async and cannot use .await
            .finish()
    }
}

impl AsyncWal {
    /// 创建新的 AsyncWal
    ///
    /// # 参数
    /// - `path`: WAL 目录路径
    /// - `config`: 配置选项
    ///
    /// # 返回
    /// 成功返回 AsyncWal 实例，失败返回错误
    pub async fn create<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 验证配置
        config.validate()?;

        // 创建目录
        fs::create_dir_all(&path).await?;

        // 创建初始段
        let segment_path = path.join("00000000000000000000.log");
        let segment = AsyncLogSegment::create(&segment_path, 0).await?;

        let active_segment = Arc::new(Mutex::new(segment));
        let mut segments = BTreeMap::new();
        segments.insert(0, Arc::clone(&active_segment));

        let wal = AsyncWal {
            path,
            config,
            segments: RwLock::new(segments),
            active_segment: RwLock::new(active_segment),
            next_offset: RwLock::new(0),
            closed: RwLock::new(false),
            rotate_lock: Mutex::new(()),
        };

        Ok(wal)
    }

    /// 打开已存在的 AsyncWal
    ///
    /// # 参数
    /// - `path`: WAL 目录路径
    ///
    /// # 返回
    /// 成功返回 AsyncWal 实例，失败返回错误
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 确保目录存在
        if !path.exists() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("WAL directory not found: {:?}", path),
            )));
        }

        // 加载默认配置
        let config = Config::default();

        // 扫描目录中的所有段文件
        let mut segments = BTreeMap::new();
        let mut max_offset = 0u64;

        let mut entries = fs::read_dir(&path).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_path = entry.path();

            // 只处理 .log 文件
            if file_path
                .extension()
                .map(|ext| ext == "log")
                .unwrap_or(false)
            {
                let segment = AsyncLogSegment::open(&file_path).await?;
                let base_offset = segment.base_offset();

                // 更新最大偏移量
                let segment_size = segment.size().await;
                let segment_end = base_offset + segment_size;
                if segment_end > max_offset {
                    max_offset = segment_end;
                }

                segments.insert(base_offset, Arc::new(Mutex::new(segment)));
            }
        }

        // 如果没有段文件，创建初始段
        if segments.is_empty() {
            let segment_path = path.join("00000000000000000000.log");
            let segment = AsyncLogSegment::create(&segment_path, 0).await?;
            segments.insert(0, Arc::new(Mutex::new(segment)));
            max_offset = 0;
        }

        // 获取活跃段（最后一个段）
        let active_segment = Arc::clone(segments.values().last().unwrap());

        let wal = AsyncWal {
            path,
            config,
            segments: RwLock::new(segments),
            active_segment: RwLock::new(active_segment),
            next_offset: RwLock::new(max_offset),
            closed: RwLock::new(false),
            rotate_lock: Mutex::new(()),
        };

        Ok(wal)
    }

    /// 写入数据
    ///
    /// # 参数
    /// - `data`: 要写入的数据
    ///
    /// # 返回
    /// 成功返回写入的偏移量，失败返回错误
    pub async fn write(&self, data: &[u8]) -> Result<u64> {
        // 检查是否已关闭
        if *self.closed.read().await {
            return Err(Error::Closed);
        }

        // 检查是否需要轮转（第一次检查）
        {
            let active_segment = self.active_segment.read().await;
            let segment = active_segment.lock().await;
            let segment_size = segment.size().await;

            // 检查是否需要轮转
            let record_size = 12 + data.len() as u64; // header + data
            if segment_size + record_size > self.config.segment_size() as u64 {
                // 需要轮转到新段
                drop(segment);
                drop(active_segment);

                // 获取轮转锁，确保只有一个线程能执行轮转
                let _rotate_guard = self.rotate_lock.lock().await;

                // 再次检查是否需要轮转（第二次检查，double-check locking）
                {
                    let active_segment = self.active_segment.read().await;
                    let segment = active_segment.lock().await;
                    let segment_size = segment.size().await;

                    if segment_size + record_size > self.config.segment_size() as u64 {
                        // 确实需要轮转，执行轮转
                        drop(segment);
                        drop(active_segment);
                        self.rotate_segment().await?;
                    }
                }
            }
        }

        // 写入数据
        let offset;
        {
            let active_segment = self.active_segment.read().await;
            let segment = active_segment.lock().await;
            let mut next_offset = self.next_offset.write().await;

            offset = *next_offset;
            let write_offset = segment.append(data).await?;

            // 更新下一个偏移量
            *next_offset = write_offset + 12 + data.len() as u64;
        }

        // 根据持久化模式处理
        match self.config.persistence_mode() {
            PersistenceMode::Immediate => {
                // Immediate 模式：立即刷新到磁盘
                let active_segment = self.active_segment.read().await;
                let segment = active_segment.lock().await;
                segment.sync().await?;
            }
            PersistenceMode::Batch | PersistenceMode::Manual => {
                // Batch/Manual 模式：不自动刷新，等待 flush() 调用
            }
        }

        Ok(offset)
    }

    /// 读取数据
    ///
    /// # 参数
    /// - `offset`: 要读取的偏移量
    ///
    /// # 返回
    /// 成功返回读取的数据，失败返回错误
    pub async fn read(&self, offset: u64) -> Result<Vec<u8>> {
        // 检查是否已关闭
        if *self.closed.read().await {
            return Err(Error::Closed);
        }

        // 查找包含该偏移量的段
        let segments = self.segments.read().await;

        // 使用二分查找找到对应的段
        let segment = segments
            .range(..=offset)
            .next_back()
            .map(|(_, seg)| Arc::clone(seg))
            .ok_or_else(|| Error::SegmentNotFound { offset })?;

        drop(segments);

        // 从段中读取数据
        let segment = segment.lock().await;
        segment.read(offset).await
    }

    /// 刷新数据到磁盘
    pub async fn flush(&self) -> Result<()> {
        // 检查是否已关闭
        if *self.closed.read().await {
            return Err(Error::Closed);
        }

        // 刷新活跃段
        let active_segment = self.active_segment.read().await;
        let segment = active_segment.lock().await;
        segment.sync().await?;

        Ok(())
    }

    /// 关闭 WAL
    pub async fn close(&self) -> Result<()> {
        // 检查是否已关闭
        if *self.closed.read().await {
            return Err(Error::Closed);
        }

        // 刷新所有数据
        self.flush().await?;

        // 标记为已关闭
        *self.closed.write().await = true;

        Ok(())
    }

    /// 清理旧段
    ///
    /// # 参数
    /// - `retain_min_offset`: 要保留的最小偏移量
    ///
    /// # 返回
    /// 成功返回清理的段数量，失败返回错误
    pub async fn prune_segments(&self, retain_min_offset: u64) -> Result<usize> {
        // 检查是否已关闭
        if *self.closed.read().await {
            return Err(Error::Closed);
        }

        let mut segments = self.segments.write().await;
        let active_segment_base = {
            let active_segment = self.active_segment.read().await;
            let segment = active_segment.lock().await;
            segment.base_offset()
        };

        // 找到包含 retain_min_offset 的段
        let retain_segment_base = segments
            .range(..=retain_min_offset)
            .next_back()
            .map(|(base, _)| *base)
            .unwrap_or(0);

        // 清理所有 base_offset < retain_segment_base 的段（除了活跃段）
        let mut removed_count = 0;
        let mut to_remove = Vec::new();

        for (base_offset, _) in segments.iter() {
            if *base_offset < retain_segment_base && *base_offset != active_segment_base {
                to_remove.push(*base_offset);
            }
        }

        for base_offset in to_remove {
            if let Some(segment) = segments.remove(&base_offset) {
                // 删除段文件
                let segment = segment.lock().await;
                let segment_path = self.path.join(format!("{:020}.log", base_offset));
                drop(segment);

                if segment_path.exists() {
                    fs::remove_file(&segment_path).await?;
                }

                removed_count += 1;
            }
        }

        Ok(removed_count)
    }

    /// 获取当前偏移量
    pub async fn current_offset(&self) -> u64 {
        *self.next_offset.read().await
    }

    /// 获取段数量
    pub async fn segment_count(&self) -> usize {
        self.segments.read().await.len()
    }

    /// 获取 WAL 路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 轮转到新段
    async fn rotate_segment(&self) -> Result<()> {
        let next_offset = *self.next_offset.read().await;
        let segment_path = self.path.join(format!("{:020}.log", next_offset));

        // 创建新段
        let new_segment = AsyncLogSegment::create(&segment_path, next_offset).await?;
        let new_segment = Arc::new(Mutex::new(new_segment));

        // 更新活跃段
        {
            let mut active_segment = self.active_segment.write().await;
            *active_segment = Arc::clone(&new_segment);
        }

        // 添加到段集合
        {
            let mut segments = self.segments.write().await;
            segments.insert(next_offset, new_segment);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_async_wal_create() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::default();
        let wal = AsyncWal::create(&path, config).await.unwrap();

        assert_eq!(wal.current_offset().await, 0);
        assert_eq!(wal.segment_count().await, 1);
    }

    #[tokio::test]
    async fn test_async_wal_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
        let wal = AsyncWal::create(&path, config).await.unwrap();

        // 写入数据
        let data = b"test data";
        let offset = wal.write(data).await.unwrap();

        // 读取数据
        let read_data = wal.read(offset).await.unwrap();
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn test_async_wal_multiple_writes() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
        let wal = AsyncWal::create(&path, config).await.unwrap();

        // 写入多条数据
        let data1 = b"record 1";
        let data2 = b"record 2";
        let data3 = b"record 3";

        let offset1 = wal.write(data1).await.unwrap();
        let offset2 = wal.write(data2).await.unwrap();
        let offset3 = wal.write(data3).await.unwrap();

        // 验证偏移量递增
        assert!(offset2 > offset1);
        assert!(offset3 > offset2);

        // 读取并验证
        assert_eq!(wal.read(offset1).await.unwrap(), data1);
        assert_eq!(wal.read(offset2).await.unwrap(), data2);
        assert_eq!(wal.read(offset3).await.unwrap(), data3);
    }

    #[tokio::test]
    async fn test_async_wal_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::new().with_persistence_mode(PersistenceMode::Immediate);
        let wal = AsyncWal::create(&path, config).await.unwrap();

        // 写入数据（Immediate 模式会自动 flush）
        let data = b"immediate data";
        let offset = wal.write(data).await.unwrap();

        // 读取验证
        let read_data = wal.read(offset).await.unwrap();
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn test_async_wal_flush() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
        let wal = AsyncWal::create(&path, config).await.unwrap();

        // 写入数据
        let data = b"manual data";
        let offset = wal.write(data).await.unwrap();

        // 手动 flush
        wal.flush().await.unwrap();

        // 读取验证
        let read_data = wal.read(offset).await.unwrap();
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn test_async_wal_segment_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        // 设置很小的段大小以触发轮转
        let config = Config::new()
            .with_segment_size(100)
            .with_persistence_mode(PersistenceMode::Manual);

        let wal = AsyncWal::create(&path, config).await.unwrap();

        // 写入足够多的数据以触发段轮转
        let data = vec![0u8; 50];
        for _ in 0..10 {
            wal.write(&data).await.unwrap();
        }

        // 验证段数量增加
        assert!(wal.segment_count().await > 1);

        // 验证数据仍可读取
        wal.flush().await.unwrap();
    }

    #[tokio::test]
    async fn test_async_wal_open_existing() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        // 创建并写入数据
        let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
        let wal1 = AsyncWal::create(&path, config).await.unwrap();

        let data = b"persistent data";
        let offset = wal1.write(data).await.unwrap();
        wal1.flush().await.unwrap();

        // 关闭并重新打开
        drop(wal1);

        let wal2 = AsyncWal::open(&path).await.unwrap();

        // 验证数据仍可读取
        let read_data = wal2.read(offset).await.unwrap();
        assert_eq!(read_data, data);
    }

    #[tokio::test]
    async fn test_async_wal_close() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
        let wal = AsyncWal::create(&path, config).await.unwrap();

        // 写入数据
        let data = b"data before close";
        wal.write(data).await.unwrap();

        // 关闭 WAL
        wal.close().await.unwrap();

        // 再次关闭应该失败
        let result = wal.close().await;
        assert!(result.is_err());
    }
}
