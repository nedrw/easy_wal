//! LogSegment 模块
//!
//! 实现段的读写功能，使用 mmap 优化性能

use crate::{Error, Result};
use crc32fast::Hasher;
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// 魔数，用于验证数据格式
const MAGIC: u32 = 0x4C4F4753; // "LOGS" in hex

/// 记录头大小：Magic(4) + Length(4) + CRC(4) = 12字节
const HEADER_SIZE: usize = 12;

/// 默认段大小（预分配大小）
const DEFAULT_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024; // 1GB

/// LogSegment 结构体
///
/// 使用 mmap 管理单个段文件的读写操作，提供真正的并发读性能
pub struct LogSegment {
    /// 段的起始偏移量
    base_offset: u64,

    /// 段文件路径
    path: PathBuf,

    /// mmap 映射（使用 RwLock 保护，读操作可以并发）
    mmap: RwLock<MmapMut>,

    /// 当前已使用大小（原子操作，无锁读取）
    size: AtomicU64,

    /// mmap 映射的总容量
    capacity: u64,

    /// 已刷新的偏移量（原子操作，用于精细刷新）
    /// 记录已经刷新到磁盘的数据位置，避免重复刷新
    flushed_offset: AtomicU64,
}

impl LogSegment {
    /// 创建新的 LogSegment
    ///
    /// # 参数
    /// - `path`: 段文件路径
    /// - `base_offset`: 段的起始偏移量
    ///
    /// # 返回
    /// 成功返回 LogSegment 实例，失败返回错误
    pub fn create<P: AsRef<Path>>(path: P, base_offset: u64) -> Result<Self> {
        Self::create_with_capacity(path, base_offset, DEFAULT_SEGMENT_SIZE)
    }

    /// 创建新的 LogSegment，指定预分配大小
    ///
    /// # 参数
    /// - `path`: 段文件路径
    /// - `base_offset`: 段的起始偏移量
    /// - `capacity`: 预分配的文件大小
    ///
    /// # 返回
    /// 成功返回 LogSegment 实例，失败返回错误
    pub fn create_with_capacity<P: AsRef<Path>>(
        path: P,
        base_offset: u64,
        capacity: u64,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 确保父目录存在
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // 创建并预分配文件大小
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        // 预分配文件空间
        file.set_len(capacity)?;

        // 创建 mmap
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        Ok(LogSegment {
            base_offset,
            path,
            mmap: RwLock::new(mmap),
            size: AtomicU64::new(0),
            capacity,
            flushed_offset: AtomicU64::new(0),
        })
    }

    /// 打开已存在的 LogSegment
    ///
    /// # 参数
    /// - `path`: 段文件路径
    ///
    /// # 返回
    /// 成功返回 LogSegment 实例，失败返回错误
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 从文件名解析 base_offset
        let base_offset = Self::parse_base_offset(&path)?;

        // 打开文件
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        // 获取文件大小
        let file_size = file.metadata()?.len();

        // 如果文件为空，预分配空间
        let capacity = if file_size == 0 {
            let cap = DEFAULT_SEGMENT_SIZE;
            file.set_len(cap)?;
            cap
        } else {
            file_size
        };

        // 创建 mmap
        let mmap = unsafe { MmapMut::map_mut(&file)? };

        // 扫描文件确定实际使用的大小
        let size = if file_size == 0 {
            0
        } else {
            Self::scan_segment_size(&mmap)?
        };

        Ok(LogSegment {
            base_offset,
            path,
            mmap: RwLock::new(mmap),
            size: AtomicU64::new(size),
            capacity,
            flushed_offset: AtomicU64::new(0),
        })
    }

    /// 扫描段文件，确定实际使用的大小
    ///
    /// 从文件开头扫描所有记录，直到遇到无效数据或文件结束
    fn scan_segment_size(mmap: &MmapMut) -> Result<u64> {
        let data = &*mmap;
        let mut offset = 0usize;

        while offset + HEADER_SIZE <= data.len() {
            // 读取 Magic
            let magic = u32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);

            if magic != MAGIC {
                break;
            }

            // 读取 Length
            let length = u32::from_be_bytes([
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);

            // 检查是否有完整的记录
            let record_size = HEADER_SIZE + length as usize;
            if offset + record_size > data.len() {
                break;
            }

            offset += record_size;
        }

        Ok(offset as u64)
    }

    /// 从文件路径解析 base_offset
    ///
    /// 文件名格式：<base_offset>.log，例如 00000000000000000000.log
    fn parse_base_offset(path: &Path) -> Result<u64> {
        let file_name = path
            .file_name()
            .ok_or_else(|| Error::Corruption {
                offset: 0,
                reason: "Invalid file name".to_string(),
            })?
            .to_str()
            .ok_or_else(|| Error::Corruption {
                offset: 0,
                reason: "Invalid file name encoding".to_string(),
            })?;

        // 移除 .log 扩展名
        let base_name = file_name
            .strip_suffix(".log")
            .ok_or_else(|| Error::Corruption {
                offset: 0,
                reason: "Invalid file extension".to_string(),
            })?;

        // 解析为 u64（使用十进制）
        let offset = u64::from_str_radix(base_name, 10).map_err(|_| Error::Corruption {
            offset: 0,
            reason: format!("Invalid base offset format: {}", base_name),
        })?;

        Ok(offset)
    }

    /// 获取段的起始偏移量
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// 获取段当前大小
    pub fn size(&self) -> u64 {
        self.size.load(Ordering::Acquire)
    }

    /// 追加数据到段
    ///
    /// # 参数
    /// - `data`: 要写入的数据
    ///
    /// # 返回
    /// 成功返回写入的偏移量，失败返回错误
    pub fn append(&self, data: &[u8]) -> Result<u64> {
        let record_size = HEADER_SIZE + data.len();

        // 获取当前大小并检查容量
        let offset = self.size.load(Ordering::Acquire);
        if offset + record_size as u64 > self.capacity {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "Segment is full",
            )));
        }

        // 计算 CRC
        let crc = crc32(data);

        // 获取写锁
        let mut mmap = self.mmap.write().unwrap();

        // 写入记录：[Magic][Length][CRC][Data]
        let offset_usize = offset as usize;
        let mmap_data = &mut *mmap;

        // 检查边界
        if offset_usize + record_size > mmap_data.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "Segment is full",
            )));
        }

        // 写入 Magic
        mmap_data[offset_usize..offset_usize + 4].copy_from_slice(&MAGIC.to_be_bytes());

        // 写入 Length
        let length = data.len() as u32;
        mmap_data[offset_usize + 4..offset_usize + 8].copy_from_slice(&length.to_be_bytes());

        // 写入 CRC
        mmap_data[offset_usize + 8..offset_usize + 12].copy_from_slice(&crc.to_be_bytes());

        // 写入 Data
        mmap_data[offset_usize + 12..offset_usize + record_size].copy_from_slice(data);

        // 更新大小（原子操作）
        self.size
            .store(offset + record_size as u64, Ordering::Release);

        // 返回绝对偏移量（base_offset + 相对偏移量）
        Ok(self.base_offset + offset)
    }

    /// 刷新数据到磁盘
    ///
    /// 使用 flush_range 精细刷新，只刷新未刷新的数据区域，而不是整个 mmap
    /// 通过 flushed_offset 记录已刷新的位置，避免重复刷新
    ///
    /// # 返回
    /// 成功返回 Ok(())，失败返回错误
    pub fn sync(&self) -> Result<()> {
        let flushed = self.flushed_offset.load(Ordering::Acquire);
        let size = self.size.load(Ordering::Acquire);

        // 如果没有数据需要刷新，直接返回
        if size == 0 || flushed >= size {
            return Ok(());
        }

        let mmap = self.mmap.read().unwrap();

        // 只刷新未刷新的数据区域（从 flushed 到 size）
        // 这样可以避免重复刷新已刷新的数据，提高性能
        let flush_size = size - flushed;
        mmap.flush_range(flushed as usize, flush_size as usize)?;

        // 更新 flushed_offset，标记这部分数据已刷新
        self.flushed_offset.store(size, Ordering::Release);

        Ok(())
    }

    /// 从段读取数据
    ///
    /// # 参数
    /// - `offset`: 要读取的偏移量（绝对偏移量）
    ///
    /// # 返回
    /// 成功返回读取的数据，失败返回错误
    pub fn read(&self, offset: u64) -> Result<Vec<u8>> {
        // 获取读锁（多个读操作可以并发）
        let mmap = self.mmap.read().unwrap();
        let size = self.size.load(Ordering::Acquire);

        // 计算相对偏移量
        let relative_offset =
            offset
                .checked_sub(self.base_offset)
                .ok_or_else(|| Error::Corruption {
                    offset,
                    reason: "Offset is before base offset".to_string(),
                })?;

        // 检查offset是否在有效范围内
        if relative_offset >= size {
            return Err(Error::Corruption {
                offset,
                reason: format!("Offset {} is beyond segment size {}", offset, size),
            });
        }

        let mmap_data = &*mmap;
        let offset_usize = relative_offset as usize;

        // 检查是否有完整的头部
        if offset_usize + HEADER_SIZE > mmap_data.len() {
            return Err(Error::Corruption {
                offset,
                reason: "Incomplete record header".to_string(),
            });
        }

        // 读取 Magic
        let magic = u32::from_be_bytes([
            mmap_data[offset_usize],
            mmap_data[offset_usize + 1],
            mmap_data[offset_usize + 2],
            mmap_data[offset_usize + 3],
        ]);

        if magic != MAGIC {
            return Err(Error::Corruption {
                offset,
                reason: format!("Invalid magic number: expected {}, got {}", MAGIC, magic),
            });
        }

        // 读取 Length
        let length = u32::from_be_bytes([
            mmap_data[offset_usize + 4],
            mmap_data[offset_usize + 5],
            mmap_data[offset_usize + 6],
            mmap_data[offset_usize + 7],
        ]);

        // 读取 CRC
        let expected_crc = u32::from_be_bytes([
            mmap_data[offset_usize + 8],
            mmap_data[offset_usize + 9],
            mmap_data[offset_usize + 10],
            mmap_data[offset_usize + 11],
        ]);

        // 检查是否有完整的数据
        let record_size = HEADER_SIZE + length as usize;
        if offset_usize + record_size > mmap_data.len() {
            return Err(Error::Corruption {
                offset,
                reason: "Incomplete record data".to_string(),
            });
        }

        // 读取 Data
        let data = mmap_data[offset_usize + HEADER_SIZE..offset_usize + record_size].to_vec();

        // 验证 CRC
        let actual_crc = crc32(&data);
        if actual_crc != expected_crc {
            return Err(Error::Corruption {
                offset,
                reason: format!(
                    "CRC mismatch: expected {}, got {}",
                    expected_crc, actual_crc
                ),
            });
        }

        Ok(data)
    }

    /// 获取段文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 关闭段，截断文件到实际大小
    ///
    /// # 返回
    /// 成功返回 Ok(())，失败返回错误
    pub fn close(&self) -> Result<()> {
        let size = self.size.load(Ordering::Acquire);

        // 截断文件到实际大小
        std::fs::OpenOptions::new()
            .write(true)
            .open(&self.path)?
            .set_len(size)?;

        Ok(())
    }
}

/// 计算 CRC32 校验码（使用 crc32fast 库）
fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_crc32() {
        // 测试 CRC32 计算
        let data = b"test data";
        let crc = crc32(data);
        assert!(crc != 0);
    }

    #[test]
    fn test_segment_create_and_open() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000000.log");

        // 创建段
        let segment = LogSegment::create(&path, 0).unwrap();
        assert_eq!(segment.base_offset(), 0);
        assert!(path.exists());

        drop(segment);

        // 重新打开
        let segment2 = LogSegment::open(&path).unwrap();
        assert_eq!(segment2.base_offset(), 0);
    }

    #[test]
    fn test_segment_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000000.log");

        let segment = LogSegment::create(&path, 0).unwrap();

        // 写入数据
        let data = b"test data";
        let offset = segment.append(data).unwrap();

        // 读取数据
        let read_data = segment.read(offset).unwrap();
        assert_eq!(read_data.as_slice(), data);
    }

    #[test]
    fn test_segment_multiple_records() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000000.log");

        let segment = LogSegment::create(&path, 0).unwrap();

        let records = vec![
            b"record 1".to_vec(),
            b"record 2".to_vec(),
            b"record 3".to_vec(),
        ];

        let mut offsets = vec![];
        for record in &records {
            let offset = segment.append(record).unwrap();
            offsets.push(offset);
        }

        // 验证所有记录都能正确读取
        for (i, offset) in offsets.iter().enumerate() {
            let read_data = segment.read(*offset).unwrap();
            assert_eq!(read_data, records[i]);
        }
    }

    #[test]
    fn test_segment_size_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000000.log");

        let segment = LogSegment::create(&path, 0).unwrap();

        assert_eq!(segment.size(), 0);

        // 写入数据
        let data = b"test data";
        segment.append(data).unwrap();

        // 验证大小增加
        let size1 = segment.size();
        assert!(size1 > 0);

        // 写入更多数据
        segment.append(b"more data").unwrap();
        let size2 = segment.size();
        assert!(size2 > size1);
    }

    #[test]
    fn test_parse_base_offset() {
        let path1 = std::path::Path::new("00000000000000000000.log");
        assert_eq!(LogSegment::parse_base_offset(path1).unwrap(), 0);

        let path2 = std::path::Path::new("00000000000000001024.log");
        assert_eq!(LogSegment::parse_base_offset(path2).unwrap(), 1024);

        let path3 = std::path::Path::new("00000000000000002048.log");
        assert_eq!(LogSegment::parse_base_offset(path3).unwrap(), 2048);
    }

    #[test]
    fn test_segment_concurrent_reads() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000000.log");

        let segment = Arc::new(LogSegment::create(&path, 0).unwrap());

        // 写入数据
        let records: Vec<Vec<u8>> = (0..10)
            .map(|i| format!("record {}", i).into_bytes())
            .collect();
        let mut offsets = vec![];
        for record in &records {
            let offset = segment.append(record).unwrap();
            offsets.push(offset);
        }

        // 并发读取
        let mut handles = vec![];
        for i in 0..10 {
            let seg = Arc::clone(&segment);
            let offset = offsets[i];
            let expected = records[i].clone();
            handles.push(thread::spawn(move || {
                let read_data = seg.read(offset).unwrap();
                assert_eq!(read_data, expected);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
