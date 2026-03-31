//! WAL (Write-Ahead Log) 模块
//!
//! 实现 WAL 对象，提供主要的读写接口

use crate::{Config, Error, LogSegment, PersistenceMode, Result};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

/// WAL 对象
///
/// 主要的入口点，管理所有的段文件并提供读写接口
pub struct Wal {
    /// WAL 目录路径
    path: PathBuf,

    /// 配置
    config: Config,

    /// 所有段的集合（按 base_offset 排序）
    segments: RwLock<BTreeMap<u64, Arc<RwLock<LogSegment>>>>,

    /// 当前活跃段
    active_segment: RwLock<Arc<RwLock<LogSegment>>>,

    /// 下一个写入偏移量（原子操作，无锁）
    next_offset: AtomicU64,

    /// 是否已关闭（原子操作，无锁）
    closed: AtomicBool,

    /// 段轮转保护锁（确保只有一个线程能执行轮转）
    rotate_lock: Mutex<()>,
}

impl fmt::Debug for Wal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Wal")
            .field("path", &self.path)
            .field("config", &self.config)
            .field("next_offset", &self.next_offset.load(Ordering::Acquire))
            .field("closed", &self.closed.load(Ordering::Acquire))
            .field("segments_count", &self.segments.read().unwrap().len())
            .finish()
    }
}

impl Wal {
    /// 创建新的 WAL
    ///
    /// # 参数
    /// - `path`: WAL 目录路径
    /// - `config`: 配置选项
    ///
    /// # 返回
    /// 成功返回 Wal 实例，失败返回错误
    pub fn create<P: AsRef<Path>>(path: P, config: Config) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 验证配置
        config.validate()?;

        // 创建目录
        fs::create_dir_all(&path)?;

        // 创建初始段
        let segment_path = path.join("00000000000000000000.log");
        let segment = LogSegment::create(&segment_path, 0)?;

        let active_segment = Arc::new(RwLock::new(segment));
        let mut segments = BTreeMap::new();
        segments.insert(0, Arc::clone(&active_segment));

        let wal = Wal {
            path,
            config,
            segments: RwLock::new(segments),
            active_segment: RwLock::new(active_segment),
            next_offset: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            rotate_lock: Mutex::new(()),
        };

        Ok(wal)
    }

    /// 打开已存在的 WAL
    ///
    /// # 参数
    /// - `path`: WAL 目录路径
    ///
    /// # 返回
    /// 成功返回 Wal 实例，失败返回错误
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
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

        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let file_path = entry.path();

            // 只处理 .log 文件
            if file_path
                .extension()
                .map(|ext| ext == "log")
                .unwrap_or(false)
            {
                let segment = LogSegment::open(&file_path)?;
                let base_offset = segment.base_offset();

                // 更新最大偏移量
                let segment_size = segment.size();
                let segment_end = base_offset + segment_size;
                if segment_end > max_offset {
                    max_offset = segment_end;
                }

                segments.insert(base_offset, Arc::new(RwLock::new(segment)));
            }
        }

        // 如果没有段文件，创建初始段
        if segments.is_empty() {
            let segment_path = path.join("00000000000000000000.log");
            let segment = LogSegment::create(&segment_path, 0)?;
            segments.insert(0, Arc::new(RwLock::new(segment)));
            max_offset = 0;
        }

        // 获取活跃段（最后一个段）
        let active_segment = Arc::clone(segments.values().last().unwrap());

        let wal = Wal {
            path,
            config,
            segments: RwLock::new(segments),
            active_segment: RwLock::new(active_segment),
            next_offset: AtomicU64::new(max_offset),
            closed: AtomicBool::new(false),
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
    pub fn write(&self, data: &[u8]) -> Result<u64> {
        // 检查是否已关闭
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        // 检查是否需要轮转（第一次检查）
        {
            let active_segment = self.active_segment.read().unwrap();
            let segment = active_segment.read().unwrap();
            let segment_size = segment.size();

            // 检查是否需要轮转
            let record_size = 12 + data.len() as u64; // header + data
            if segment_size + record_size > self.config.segment_size() as u64 {
                // 需要轮转到新段
                drop(segment);
                drop(active_segment);

                // 获取轮转锁，确保只有一个线程能执行轮转
                let _rotate_guard = self.rotate_lock.lock().unwrap();

                // 再次检查是否需要轮转（第二次检查，double-check locking）
                {
                    let active_segment = self.active_segment.read().unwrap();
                    let segment = active_segment.read().unwrap();
                    let segment_size = segment.size();

                    if segment_size + record_size > self.config.segment_size() as u64 {
                        // 确实需要轮转，执行轮转
                        drop(segment);
                        drop(active_segment);
                        self.rotate_segment()?;
                    }
                }
            }
        }

        // 写入数据
        let offset;
        {
            let active_segment = self.active_segment.read().unwrap();
            let segment = active_segment.write().unwrap();

            offset = self.next_offset.load(Ordering::Acquire);
            let write_offset = segment.append(data)?;

            // 更新下一个偏移量（原子操作）
            self.next_offset
                .store(write_offset + 12 + data.len() as u64, Ordering::Release);
        }

        // 根据持久化模式处理
        match self.config.persistence_mode() {
            PersistenceMode::Immediate => {
                // Immediate 模式：立即刷新到磁盘
                let active_segment = self.active_segment.read().unwrap();
                let segment = active_segment.read().unwrap();
                segment.sync()?;
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
    pub fn read(&self, offset: u64) -> Result<Vec<u8>> {
        // 检查是否已关闭
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        // 查找包含该偏移量的段
        let segments = self.segments.read().unwrap();

        // 使用二分查找找到对应的段
        let segment = segments
            .range(..=offset)
            .next_back()
            .map(|(_, seg)| Arc::clone(seg))
            .ok_or_else(|| Error::SegmentNotFound { offset })?;

        drop(segments);

        // 从段中读取数据
        let segment = segment.read().unwrap();
        segment.read(offset)
    }

    /// 刷新数据到磁盘
    pub fn flush(&self) -> Result<()> {
        // 检查是否已关闭
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        // 刷新当前活跃段到磁盘
        let active_segment = self.active_segment.read().unwrap();
        let segment = active_segment.read().unwrap();
        segment.sync()?;

        Ok(())
    }

    /// 关闭 WAL
    pub fn close(&self) -> Result<()> {
        self.closed.store(true, Ordering::Release);
        Ok(())
    }

    /// 获取 WAL 路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 清理旧段，释放磁盘空间
    ///
    /// # 参数
    /// - `retain_min_offset`: 保留的最小offset，小于此offset的段将被清理
    ///
    /// # 返回
    /// 成功返回 Ok(())，失败返回错误
    ///
    /// # 注意
    /// - 活跃段不会被清理（即使offset小于retain_min_offset）
    /// - 清理后，已删除段的数据将无法访问（返回 SegmentNotFound 错误）
    pub fn prune_segments(&self, retain_min_offset: u64) -> Result<()> {
        // 检查是否已关闭
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Closed);
        }

        // 获取活跃段的base_offset（活跃段不应该被删除）
        let active_base_offset = {
            let active_segment = self.active_segment.read().unwrap();
            let segment = active_segment.read().unwrap();
            segment.base_offset()
        };

        // 找到包含retain_min_offset的段（这个段及之后的段应该被保留）
        // 这样可以确保包含retain_min_offset数据的段不会被清理
        let retain_segment_base_offset: Option<u64> = {
            let segments = self.segments.read().unwrap();
            segments
                .range(..=retain_min_offset)
                .next_back()
                .map(|(base_offset, _)| *base_offset)
        };

        // 找到所有需要删除的段
        // 如果找到了retain_segment，清理所有base_offset < retain_segment_base_offset的段
        // 如果没找到retain_segment，说明retain_min_offset超出了所有段的范围，不清理任何段
        let segments_to_delete: Vec<u64> = {
            let segments = self.segments.read().unwrap();
            if let Some(retain_base_offset) = retain_segment_base_offset {
                // 清理所有base_offset < retain_base_offset的段（除了活跃段）
                segments
                    .keys()
                    .filter(|&base_offset| {
                        *base_offset < retain_base_offset && *base_offset != active_base_offset
                    })
                    .copied()
                    .collect()
            } else {
                // 没有找到包含retain_min_offset的段，不清理任何段
                vec![]
            }
        };

        // 如果没有需要删除的段，直接返回
        if segments_to_delete.is_empty() {
            return Ok(());
        }

        // 从内存中移除这些段
        {
            let mut segments = self.segments.write().unwrap();
            for base_offset in &segments_to_delete {
                segments.remove(base_offset);
            }
        }

        // 删除对应的段文件
        for base_offset in segments_to_delete {
            let segment_path = self.path.join(format!("{:020}.log", base_offset));
            std::fs::remove_file(&segment_path)?;
        }

        Ok(())
    }

    /// 轮转到新段
    fn rotate_segment(&self) -> Result<()> {
        let next_offset = self.next_offset.load(Ordering::Acquire);
        let segment_path = self.path.join(format!("{:020}.log", next_offset));

        // 创建新段
        let new_segment = LogSegment::create(&segment_path, next_offset)?;
        let new_segment = Arc::new(RwLock::new(new_segment));

        // 更新活跃段
        {
            let mut active_segment = self.active_segment.write().unwrap();
            *active_segment = Arc::clone(&new_segment);
        }

        // 添加到段集合
        {
            let mut segments = self.segments.write().unwrap();
            segments.insert(next_offset, new_segment);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_create() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::default();
        let wal = Wal::create(&path, config).unwrap();

        assert!(path.exists());
        assert_eq!(wal.path(), path);
    }

    #[test]
    fn test_wal_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::default();
        let wal = Wal::create(&path, config).unwrap();

        let data = b"test data";
        let offset = wal.write(data).unwrap();

        let read_data = wal.read(offset).unwrap();
        assert_eq!(read_data.as_slice(), data);
    }

    #[test]
    fn test_wal_multiple_writes() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::default();
        let wal = Wal::create(&path, config).unwrap();

        let records = vec![b"record 1", b"record 2", b"record 3"];

        let mut offsets = vec![];
        for record in &records {
            let offset = wal.write(*record).unwrap();
            offsets.push(offset);
        }

        for (i, offset) in offsets.iter().enumerate() {
            let read_data = wal.read(*offset).unwrap();
            assert_eq!(read_data, records[i]);
        }
    }

    #[test]
    fn test_wal_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("wal");

        let config = Config::new().with_persistence_mode(PersistenceMode::Immediate);

        // 写入数据
        let offset = {
            let wal = Wal::create(&path, config).unwrap();
            let data = b"persistent data";
            wal.write(data).unwrap()
        };

        // 重新打开并读取
        let wal = Wal::open(&path).unwrap();
        let read_data = wal.read(offset).unwrap();
        assert_eq!(read_data.as_slice(), b"persistent data");
    }
}
