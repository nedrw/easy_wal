//! 日志读取器 - 顺序读取组件
//!
//! # 教学价值
//! - 学习迭代器模式
//! - 学习状态管理
//! - 学习资源清理

use super::{FileStorage, SegmentConfig, SegmentManager, Storage, crc32, format};
use crate::prelude::*;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 读取位置信息
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadPosition {
    /// 段 ID
    pub segment_id: u64,
    /// 段内偏移量
    pub offset: u64,
}

/// 日志读取器配置
#[derive(Debug, Clone)]
pub struct LogReaderConfig {
    /// 段配置
    pub segment_config: SegmentConfig,
    /// 批量读取大小
    pub batch_size: usize,
    /// 读取缓冲区大小
    pub read_buffer_size: usize,
}

impl Default for LogReaderConfig {
    fn default() -> Self {
        Self {
            segment_config: SegmentConfig::default(),
            batch_size: 100,
            read_buffer_size: 64 * 1024, // 64KB
        }
    }
}

impl LogReaderConfig {
    pub fn with_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.segment_config = SegmentConfig::new(dir);
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

/// 日志读取器
///
/// 提供顺序读取功能，支持遍历所有段的数据。
pub struct LogReader {
    config: LogReaderConfig,
    segment_manager: RwLock<SegmentManager>,
    /// 当前读取位置
    position: RwLock<ReadPosition>,
    /// 当前活跃存储
    active_storage: RwLock<Option<Arc<FileStorage>>>,
}

impl LogReader {
    /// 创建日志读取器
    pub async fn new(config: LogReaderConfig) -> Result<Self> {
        let mut segment_manager = SegmentManager::new(config.segment_config.clone())
            .map_err(|e| Error::Generic(format!("Failed to create segment manager: {}", e)))?;

        // 如果没有现有段，创建第一个段
        if segment_manager.segments().is_empty() {
            segment_manager.create_segment().ok();
        }

        Ok(Self {
            config,
            segment_manager: RwLock::new(segment_manager),
            position: RwLock::new(ReadPosition {
                segment_id: 1,
                offset: format::SEGMENT_HEADER_SIZE, // Skip header
            }),
            active_storage: RwLock::new(None),
        })
    }

    /// 获取或创建指定段的存储
    async fn get_storage_for_segment(&self, segment_id: u64) -> Result<Arc<FileStorage>> {
        // 先尝试获取现有存储
        {
            let storage = self.active_storage.read().await;
            if let Some(ref s) = *storage {
                let manager = self.segment_manager.read().await;
                if manager.active_id() == segment_id {
                    return Ok(s.clone());
                }
            }
        }

        // 需要加载指定段
        let manager = self.segment_manager.read().await;
        let path = manager
            .segment_path(segment_id)
            .ok_or_else(|| Error::Generic(format!("Segment {} not found", segment_id)))?;

        let storage = Arc::new(
            FileStorage::new(&path)
                .await
                .map_err(|e| Error::Generic(format!("Failed to open storage: {}", e)))?,
        );

        // 保存到活跃存储
        let mut active = self.active_storage.write().await;
        *active = Some(storage.clone());

        Ok(storage)
    }

    /// 读取单条数据
    ///
    /// # 参数
    /// - `offset`: 读取偏移量
    /// - `length`: 读取长度
    ///
    /// # 返回
    /// 读取到的数据
    pub async fn read(&self, offset: u64, length: u64) -> Result<Vec<u8>> {
        let pos = self.position.read().await;
        let storage = self.get_storage_for_segment(pos.segment_id).await?;
        storage.read(offset, length).await
    }

    /// 读取指定位置的数据
    pub async fn read_at(&self, segment_id: u64, offset: u64, length: u64) -> Result<Vec<u8>> {
        let storage = self.get_storage_for_segment(segment_id).await?;
        storage.read(offset, length).await
    }

    /// 批量顺序读取
    ///
    /// 从当前位置开始批量读取数据。
    /// 读取完成后会自动更新位置。
    ///
    /// # 参数
    /// - `max_count`: 最大读取条数
    ///
    /// # 返回
    /// 读取到的数据列表
    pub async fn read_batch(&self, max_count: usize) -> Result<Vec<Vec<u8>>> {
        let mut results = Vec::with_capacity(max_count);
        let count = max_count.min(self.config.batch_size);

        for _ in 0..count {
            match self.read_next().await {
                Ok(data) => results.push(data),
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(results)
    }

    /// 读取原始数据（不解析格式）
    ///
    /// 用于预读优化，直接从存储读取原始字节。
    ///
    /// # 跨段读取特性
    /// - 当到达当前段末尾（读取不足预期长度）时，自动切换到下一个段继续读取
    /// - 合并多个段的数据，确保返回足够长度的原始字节
    /// - 适用于 ReadCoordinator 的预读缓冲区策略
    /// - 保证多段 WAL 文件恢复后能够完整读取所有数据
    pub async fn read_raw(&self, length: usize) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        let mut remaining = length;

        while remaining > 0 {
            let (segment_id, offset) = {
                let pos = self.position.read().await;
                (pos.segment_id, pos.offset)
            };

            let storage = match self.get_storage_for_segment(segment_id).await {
                Ok(s) => s,
                Err(Error::Generic(_)) => {
                    // 当前段不存在，尝试切换到下一个段
                    let next_segment_id = segment_id + 1;
                    let has_next = {
                        let manager = self.segment_manager.read().await;
                        manager.get_segment(next_segment_id).is_some()
                    }; // manager 在此自动释放
                    if has_next {
                        let mut pos = self.position.write().await;
                        pos.segment_id = next_segment_id;
                        pos.offset = format::SEGMENT_HEADER_SIZE;
                        continue;
                    } else {
                        // 没有下一个段，返回已读取的数据
                        if result.is_empty() {
                            return Ok(Vec::new());
                        } else {
                            return Ok(result);
                        }
                    }
                }
                Err(e) => return Err(e),
            };

            // 从当前段读取数据
            let data = storage.read(offset, remaining as u64).await?;
            let read_len = data.len();

            if read_len == 0 {
                // 到达段末尾，尝试切换到下一个段
                let next_segment_id = segment_id + 1;
                let has_next = {
                    let manager = self.segment_manager.read().await;
                    manager.get_segment(next_segment_id).is_some()
                }; // manager 在此自动释放
                if has_next {
                    let mut pos = self.position.write().await;
                    pos.segment_id = next_segment_id;
                    pos.offset = format::SEGMENT_HEADER_SIZE;
                    continue;
                } else {
                    // 没有下一个段，返回已读取的数据
                    return Ok(result);
                }
            }

            // 添加读取的数据到结果
            result.extend_from_slice(&data);
            remaining -= read_len;

            // 更新位置
            let mut pos = self.position.write().await;
            pos.offset += read_len as u64;
        }

        Ok(result)
    }

    /// 获取指定段的路径
    pub fn segment_path(&self, segment_id: u64) -> Option<std::path::PathBuf> {
        // 同步获取 manager
        let manager = self.segment_manager.blocking_read();
        manager.segment_path(segment_id)
    }

    /// 读取下一条数据
    ///
    /// 从当前位置读取一条数据，并更新位置。
    /// 格式：[4B Magic][4B Length][4B CRC32][Data...]
    pub async fn read_next(&self) -> Result<Vec<u8>> {
        loop {
            // 先获取当前位置（释放锁后再做IO）
            let (segment_id, offset) = {
                let pos = self.position.read().await;
                (pos.segment_id, pos.offset)
            };

            let storage = self.get_storage_for_segment(segment_id).await?;

            // 预读整个记录头 (12 bytes: 4 magic + 4 length + 4 crc)
            // 优化：从 4 次独立 IO 减少为 2 次（1 次预读头 + 1 次读数据）
            let header = storage.read(offset, format::RECORD_HEADER_SIZE).await?;

            // 如果读取不足记录头大小，说明到达段末尾
            if header.len() < format::RECORD_HEADER_SIZE as usize {
                // 尝试切换到下一个段
                let next_segment_id = segment_id + 1;

                // 检查是否有下一个段
                let has_next = {
                    let manager = self.segment_manager.read().await;
                    manager.get_segment(next_segment_id).is_some()
                }; // manager 在此自动释放
                if has_next {
                    let mut pos = self.position.write().await;
                    pos.segment_id = next_segment_id;
                    pos.offset = format::SEGMENT_HEADER_SIZE;
                    continue; // 继续循环，读取下一个段
                } else {
                    // 没有下一个段，返回 EOF
                    return Err(Error::Eof);
                }
            }

            // 解析 Magic
            let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
            if magic != format::RECORD_MAGIC {
                return Err(Error::Generic(format!(
                    "Invalid record magic: {:08x}",
                    magic
                )));
            }

            // 解析 Length
            let length = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as u64;

            // 验证长度合理性
            if length == 0 || length > format::MAX_RECORD_SIZE {
                return Err(Error::Generic(format!("Invalid record length: {}", length)));
            }

            // 解析 CRC32
            let expected_crc = u32::from_be_bytes([header[8], header[9], header[10], header[11]]);

            // 读取数据
            let data_offset = offset + format::RECORD_HEADER_SIZE;
            let data = storage.read(data_offset, length).await?;

            // 完整性保护：验证实际读取的字节数与声明的长度一致
            // 防止 IO 中途文件被截断导致读到不完整数据
            if data.len() as u64 != length {
                return Err(Error::Generic(format!(
                    "Incomplete read: expected {} bytes, got {}",
                    length,
                    data.len()
                )));
            }

            // 验证 CRC32
            let actual_crc = crc32(&data);
            if actual_crc != expected_crc {
                return Err(Error::Generic(format!(
                    "CRC32 mismatch: expected {:08x}, got {:08x}",
                    expected_crc, actual_crc
                )));
            }

            // 更新位置（IO完成后再次获取锁）
            let mut write_pos = self.position.write().await;
            write_pos.offset = data_offset + length;

            return Ok(data);
        }
    }

    /// 跳到指定位置
    pub async fn seek(&self, segment_id: u64, offset: u64) {
        let mut pos = self.position.write().await;
        pos.segment_id = segment_id;
        pos.offset = offset;
    }

    /// 跳到开头
    pub async fn seek_to_start(&self) {
        self.seek(1, format::SEGMENT_HEADER_SIZE).await;
    }

    /// 获取当前位置
    pub async fn position(&self) -> ReadPosition {
        *self.position.read().await
    }

    /// 获取所有段信息
    pub async fn segments(&self) -> Vec<super::SegmentMeta> {
        let manager = self.segment_manager.read().await;
        manager.segments().to_vec()
    }

    /// 获取段数量
    pub async fn segment_count(&self) -> usize {
        let manager = self.segment_manager.read().await;
        manager.segments().len()
    }

    /// 关闭读取器
    pub async fn close(&self) -> Result<()> {
        let mut active = self.active_storage.write().await;
        *active = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{SegmentConfig, SegmentManager};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_read_empty_segment() {
        let temp_dir = tempdir().unwrap();
        let config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = LogReader::new(config).await.unwrap();

        let pos = reader.position().await;
        assert_eq!(pos.segment_id, 1);
        // New format: offset starts after 16-byte segment header
        assert_eq!(pos.offset, format::SEGMENT_HEADER_SIZE);
    }

    #[tokio::test]
    async fn test_seek() {
        let temp_dir = tempdir().unwrap();
        let config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = LogReader::new(config).await.unwrap();

        reader.seek(5, 100).await;

        let pos = reader.position().await;
        assert_eq!(pos.segment_id, 5);
        assert_eq!(pos.offset, 100);
    }

    // 注意：读写集成测试在 WalManager 中进行
    // LogReader 的 read_next 使用长度前缀格式，需与 WalManager 配合使用

    #[tokio::test]
    async fn test_segment_count() {
        let temp_dir = tempdir().unwrap();

        // 手动创建多个段文件
        let segment_config = SegmentConfig::new(temp_dir.path());
        let mut segment_manager = SegmentManager::new(segment_config).unwrap();

        // 创建3个段
        segment_manager.create_segment().unwrap();
        segment_manager.create_segment().unwrap();
        segment_manager.create_segment().unwrap();

        // 测试 LogReader 的段计数功能
        let reader_config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = LogReader::new(reader_config).await.unwrap();

        let count = reader.segment_count().await;
        assert_eq!(count, 3);
    }
}
