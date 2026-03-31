//! WAL (Write-Ahead Log) 模块
//!
//! 实现 WAL 对象，提供主要的读写接口

use crate::{Config, Error, LogSegment, PersistenceMode, Result};
use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

/// WAL 内部状态
///
/// 所有可变状态集中在一个结构中，用单一的 RwLock 保护
struct WalInner {
    /// 所有段的集合（按 base_offset 排序）
    segments: BTreeMap<u64, Arc<RwLock<LogSegment>>>,

    /// 当前活跃段
    active_segment: Arc<RwLock<LogSegment>>,

    /// 下一个写入偏移量
    next_offset: u64,
}

/// WAL 对象
///
/// 主要的入口点，管理所有的段文件并提供读写接口
pub struct Wal {
    /// WAL 目录路径
    path: PathBuf,

    /// 配置
    config: Config,

    /// 内部状态（单一 RwLock 保护）
    inner: RwLock<WalInner>,

    /// 是否已关闭（原子操作，无锁）
    closed: AtomicBool,
}

impl fmt::Debug for Wal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let inner = self.inner.read().unwrap();
        f.debug_struct("Wal")
            .field("path", &self.path)
            .field("config", &self.config)
            .field("next_offset", &inner.next_offset)
            .field("closed", &self.closed.load(Ordering::Acquire))
            .field("segments_count", &inner.segments.len())
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

        let inner = WalInner {
            segments,
            active_segment,
            next_offset: 0,
        };

        let wal = Wal {
            path,
            config,
            inner: RwLock::new(inner),
            closed: AtomicBool::new(false),
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

        let inner = WalInner {
            segments,
            active_segment,
            next_offset: max_offset,
        };

        let wal = Wal {
            path,
            config,
            inner: RwLock::new(inner),
            closed: AtomicBool::new(false),
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

        let offset;
        let need_sync;

        // 使用单一写锁保护所有写入操作
        {
            let mut inner = self.inner.write().unwrap();

            // 检查是否需要轮转
            let segment_size = {
                let segment = inner.active_segment.read().unwrap();
                segment.size()
            };

            let record_size = 12 + data.len() as u64; // header + data
            if segment_size + record_size > self.config.segment_size() as u64 {
                // 需要轮转到新段
                let next_offset = inner.next_offset;
                let segment_path = self.path.join(format!("{:020}.log", next_offset));

                // 创建新段
                let new_segment = LogSegment::create(&segment_path, next_offset)?;
                let new_segment = Arc::new(RwLock::new(new_segment));

                // 更新活跃段和段集合
                inner.active_segment = Arc::clone(&new_segment);
                inner.segments.insert(next_offset, new_segment);
            }

            // 写入数据
            offset = inner.next_offset;
            let write_offset = {
                let segment = inner.active_segment.write().unwrap();
                segment.append(data)?
            };

            // 更新下一个偏移量
            inner.next_offset = write_offset + 12 + data.len() as u64;

            // 根据持久化模式决定是否同步
            need_sync = self.config.persistence_mode() == PersistenceMode::Immediate;
        }

        // 如果需要立即同步，在锁外执行（减少锁持有时间）
        if need_sync {
            let inner = self.inner.read().unwrap();
            let segment = inner.active_segment.read().unwrap();
            segment.sync()?;
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

        // 使用读锁查找段
        let segment = {
            let inner = self.inner.read().unwrap();

            // 使用二分查找找到对应的段
            inner
                .segments
                .range(..=offset)
                .next_back()
                .map(|(_, seg)| Arc::clone(seg))
                .ok_or_else(|| Error::SegmentNotFound { offset })?
        };

        // 从段中读取数据（段有自己的 RwLock，允许并发读）
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
        let inner = self.inner.read().unwrap();
        let segment = inner.active_segment.read().unwrap();
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

        // 使用写锁保护整个清理操作
        let segments_to_delete: Vec<u64> = {
            let inner = self.inner.write().unwrap();

            // 获取活跃段的base_offset（活跃段不应该被删除）
            let active_base_offset = {
                let segment = inner.active_segment.read().unwrap();
                segment.base_offset()
            };

            // 找到包含retain_min_offset的段（这个段及之后的段应该被保留）
            let retain_segment_base_offset: Option<u64> = inner
                .segments
                .range(..=retain_min_offset)
                .next_back()
                .map(|(base_offset, _)| *base_offset);

            // 找到所有需要删除的段
            if let Some(retain_base_offset) = retain_segment_base_offset {
                // 清理所有base_offset < retain_base_offset的段（除了活跃段）
                inner
                    .segments
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
            let mut inner = self.inner.write().unwrap();
            for base_offset in &segments_to_delete {
                inner.segments.remove(base_offset);
            }
        }

        // 删除对应的段文件
        for base_offset in segments_to_delete {
            let segment_path = self.path.join(format!("{:020}.log", base_offset));
            std::fs::remove_file(&segment_path)?;
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
