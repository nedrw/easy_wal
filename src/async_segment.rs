//! AsyncLogSegment 模块
//!
//! 实现异步段的读写功能，使用 tokio::fs 进行异步文件操作

use crate::{Error, Result};
use crc32fast::Hasher;
use std::path::{Path, PathBuf};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Mutex;

/// 魔数，用于验证数据格式
const MAGIC: u32 = 0x4C4F4753; // "LOGS" in hex

/// 记录头大小：Magic(4) + Length(4) + CRC(4) = 12字节
const HEADER_SIZE: usize = 12;

/// AsyncLogSegment 结构体
///
/// 管理单个段文件的异步读写操作
pub struct AsyncLogSegment {
    /// 段的起始偏移量
    base_offset: u64,

    /// 段文件路径
    path: PathBuf,

    /// 文件句柄（使用 tokio Mutex 保证异步环境线程安全）
    file: Mutex<File>,

    /// 当前段大小
    size: Mutex<u64>,
}

impl AsyncLogSegment {
    /// 创建新的 AsyncLogSegment
    ///
    /// # 参数
    /// - `path`: 段文件路径
    /// - `base_offset`: 段的起始偏移量
    ///
    /// # 返回
    /// 成功返回 AsyncLogSegment 实例，失败返回错误
    pub async fn create<P: AsRef<Path>>(path: P, base_offset: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 确保父目录存在
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        // 创建新文件
        let mut file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(&path)
            .await?;

        // Seek to start position to ensure correct file position
        file.seek(SeekFrom::Start(0)).await?;

        let segment = AsyncLogSegment {
            base_offset,
            path,
            file: Mutex::new(file),
            size: Mutex::new(0),
        };

        Ok(segment)
    }

    /// 打开已存在的 AsyncLogSegment
    ///
    /// # 参数
    /// - `path`: 段文件路径
    ///
    /// # 返回
    /// 成功返回 AsyncLogSegment 实例，失败返回错误
    pub async fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 打开文件（读写模式）
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await?;

        // 解析文件名获取 base_offset
        let base_offset = Self::parse_base_offset(&path)?;

        // 计算段大小
        let metadata = tokio::fs::metadata(&path).await?;
        let size = metadata.len();

        // Seek to start position to ensure correct file position
        file.seek(SeekFrom::Start(0)).await?;

        let segment = AsyncLogSegment {
            base_offset,
            path,
            file: Mutex::new(file),
            size: Mutex::new(size),
        };

        Ok(segment)
    }

    /// 从文件名解析 base_offset
    ///
    /// 文件名格式：00000000000000000000.log（20位数字）
    fn parse_base_offset(path: &Path) -> Result<u64> {
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid file name",
                ))
            })?;

        // 提取数字部分（去掉 .log 后缀）
        let offset_str = file_name.strip_suffix(".log").ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "File name must end with .log",
            ))
        })?;

        // 解析为 u64
        let base_offset = offset_str.parse::<u64>().map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid offset format in file name",
            ))
        })?;

        Ok(base_offset)
    }

    /// 追加数据到段
    ///
    /// # 参数
    /// - `data`: 要写入的数据
    ///
    /// # 返回
    /// 成功返回写入的偏移量（相对于 WAL 的全局偏移量），失败返回错误
    pub async fn append(&self, data: &[u8]) -> Result<u64> {
        // 计算数据长度和 CRC
        let length = data.len() as u32;
        let crc = Self::calculate_crc(data);

        // 获取当前段大小作为写入偏移量
        let mut size = self.size.lock().await;
        let write_offset = *size;

        // 构造记录：[Magic][Length][CRC][Data]
        let mut record = Vec::with_capacity(HEADER_SIZE + data.len());
        record.extend_from_slice(&MAGIC.to_be_bytes());
        record.extend_from_slice(&length.to_be_bytes());
        record.extend_from_slice(&crc.to_be_bytes());
        record.extend_from_slice(data);

        // 写入文件
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(write_offset)).await?;
        file.write_all(&record).await?;

        // 更新段大小
        *size += record.len() as u64;

        // 返回全局偏移量
        Ok(self.base_offset + write_offset)
    }

    /// 从段读取数据
    ///
    /// # 参数
    /// - `offset`: 要读取的全局偏移量
    ///
    /// # 返回
    /// 成功返回读取的数据，失败返回错误
    pub async fn read(&self, offset: u64) -> Result<Vec<u8>> {
        // 计算段内偏移量
        let segment_offset = offset - self.base_offset;

        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(segment_offset)).await?;

        // 读取记录头
        let mut header = [0u8; HEADER_SIZE];
        file.read_exact(&mut header).await?;

        // 解析 header
        let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
        let length = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);
        let expected_crc = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);

        // 验证魔数
        if magic != MAGIC {
            return Err(Error::Corruption {
                offset: segment_offset,
                reason: "Invalid magic number".to_string(),
            });
        }

        // 读取数据
        let mut data = vec![0u8; length as usize];
        file.read_exact(&mut data).await?;

        // 验证 CRC
        let actual_crc = Self::calculate_crc(&data);
        if actual_crc != expected_crc {
            return Err(Error::Corruption {
                offset: segment_offset,
                reason: "CRC mismatch".to_string(),
            });
        }

        Ok(data)
    }

    /// 同步数据到磁盘
    ///
    /// # 返回
    /// 成功返回 (), 失败返回错误
    pub async fn sync(&self) -> Result<()> {
        let file = self.file.lock().await;
        file.sync_data().await?;
        Ok(())
    }

    /// 获取段的起始偏移量
    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }

    /// 获取当前段大小
    pub async fn size(&self) -> u64 {
        *self.size.lock().await
    }

    /// 计算数据的 CRC32
    fn calculate_crc(data: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(data);
        hasher.finalize()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_async_segment_create() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000000.log");

        let segment = AsyncLogSegment::create(&path, 0).await.unwrap();
        assert_eq!(segment.base_offset(), 0);
        assert_eq!(segment.size().await, 0);
    }

    #[tokio::test]
    async fn test_async_segment_open() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000100.log");

        // 先创建段
        AsyncLogSegment::create(&path, 100).await.unwrap();

        // 再打开段
        let segment = AsyncLogSegment::open(&path).await.unwrap();
        assert_eq!(segment.base_offset(), 100);
    }

    #[tokio::test]
    async fn test_async_segment_append_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000000.log");

        let segment = AsyncLogSegment::create(&path, 0).await.unwrap();

        // 写入数据
        let data = b"test data";
        let offset = segment.append(data).await.unwrap();
        assert_eq!(offset, 0);

        // 读取数据
        let read_data = segment.read(offset).await.unwrap();
        assert_eq!(read_data, data);

        // 验证段大小
        assert_eq!(segment.size().await, HEADER_SIZE as u64 + data.len() as u64);
    }

    #[tokio::test]
    async fn test_async_segment_multiple_records() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000000.log");

        let segment = AsyncLogSegment::create(&path, 0).await.unwrap();

        // 写入多条记录
        let data1 = b"record 1";
        let data2 = b"record 2";
        let data3 = b"record 3";

        let offset1 = segment.append(data1).await.unwrap();
        let offset2 = segment.append(data2).await.unwrap();
        let offset3 = segment.append(data3).await.unwrap();

        // 验证偏移量递增
        assert!(offset2 > offset1);
        assert!(offset3 > offset2);

        // 读取并验证数据
        assert_eq!(segment.read(offset1).await.unwrap(), data1);
        assert_eq!(segment.read(offset2).await.unwrap(), data2);
        assert_eq!(segment.read(offset3).await.unwrap(), data3);
    }

    #[tokio::test]
    async fn test_async_segment_crc_validation() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("00000000000000000000.log");

        let segment = AsyncLogSegment::create(&path, 0).await.unwrap();

        // 写入数据
        let data = b"important data";
        let offset = segment.append(data).await.unwrap();

        // 读取应该成功
        let result = segment.read(offset).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }
}
