//! LogSegment 模块
//!
//! 实现段的读写功能

use crate::{Error, Result};
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// 魔数，用于验证数据格式
const MAGIC: u32 = 0x4C4F4753; // "LOGS" in hex

/// 记录头大小：Magic(4) + Length(4) + CRC(4) = 12字节
const HEADER_SIZE: usize = 12;

/// LogSegment 结构体
///
/// 管理单个段文件的读写操作
pub struct LogSegment {
    /// 段的起始偏移量
    base_offset: u64,

    /// 段文件路径
    path: PathBuf,

    /// 文件句柄（使用 Mutex 保证线程安全）
    file: Mutex<File>,

    /// 当前段大小
    size: Mutex<u64>,
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
        let path = path.as_ref().to_path_buf();

        // 确保父目录存在
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // 创建文件
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;

        let segment = LogSegment {
            base_offset,
            path,
            file: Mutex::new(file),
            size: Mutex::new(0),
        };

        Ok(segment)
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
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        // 获取文件大小
        let size = file.seek(SeekFrom::End(0))?;

        let segment = LogSegment {
            base_offset,
            path,
            file: Mutex::new(file),
            size: Mutex::new(size),
        };

        Ok(segment)
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
        *self.size.lock().unwrap()
    }

    /// 追加数据到段
    ///
    /// # 参数
    /// - `data`: 要写入的数据
    ///
    /// # 返回
    /// 成功返回写入的偏移量，失败返回错误
    pub fn append(&mut self, data: &[u8]) -> Result<u64> {
        let mut file = self.file.lock().unwrap();
        let mut size = self.size.lock().unwrap();

        // 计算写入位置（相对于段起始的偏移量）
        let offset = *size;

        // 计算 CRC
        let crc = crc32(data);

        // 写入记录：[Magic][Length][CRC][Data]
        let length = data.len() as u32;

        // 写入 Magic
        file.write_all(&MAGIC.to_be_bytes())?;

        // 写入 Length
        file.write_all(&length.to_be_bytes())?;

        // 写入 CRC
        file.write_all(&crc.to_be_bytes())?;

        // 写入 Data
        file.write_all(data)?;

        // 更新段大小
        let record_size = HEADER_SIZE + data.len();
        *size += record_size as u64;

        // 返回绝对偏移量（base_offset + 相对偏移量）
        Ok(self.base_offset + offset)
    }

    /// 刷新数据到磁盘
    ///
    /// # 返回
    /// 成功返回 Ok(())，失败返回错误
    pub fn sync(&self) -> Result<()> {
        let file = self.file.lock().unwrap();
        file.sync_data()?;
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
        let mut file = self.file.lock().unwrap();
        let size = self.size.lock().unwrap();

        // 计算相对偏移量
        let relative_offset =
            offset
                .checked_sub(self.base_offset)
                .ok_or_else(|| Error::Corruption {
                    offset,
                    reason: "Offset is before base offset".to_string(),
                })?;

        // 检查offset是否在有效范围内
        if relative_offset >= *size {
            return Err(Error::Corruption {
                offset,
                reason: format!("Offset {} is beyond segment size {}", offset, *size),
            });
        }

        // 释放size锁，避免死锁
        drop(size);

        // 定位到读取位置
        file.seek(SeekFrom::Start(relative_offset))?;

        // 读取 Magic
        let mut magic_bytes = [0u8; 4];
        file.read_exact(&mut magic_bytes)?;
        let magic = u32::from_be_bytes(magic_bytes);

        if magic != MAGIC {
            return Err(Error::Corruption {
                offset,
                reason: format!("Invalid magic number: expected {}, got {}", MAGIC, magic),
            });
        }

        // 读取 Length
        let mut length_bytes = [0u8; 4];
        file.read_exact(&mut length_bytes)?;
        let length = u32::from_be_bytes(length_bytes);

        // 读取 CRC
        let mut crc_bytes = [0u8; 4];
        file.read_exact(&mut crc_bytes)?;
        let expected_crc = u32::from_be_bytes(crc_bytes);

        // 读取 Data
        let mut data = vec![0u8; length as usize];
        file.read_exact(&mut data)?;

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

        let mut segment = LogSegment::create(&path, 0).unwrap();

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

        let mut segment = LogSegment::create(&path, 0).unwrap();

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

        let mut segment = LogSegment::create(&path, 0).unwrap();

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
}
