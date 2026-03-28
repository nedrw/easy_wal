//! WAL 恢复机制 - 崩溃恢复和 Checkpoint
//!
//! # 教学价值
//! - 学习崩溃恢复设计
//! - 学习检查点机制
//! - 学习数据完整性验证

use crate::error::Error;
use crate::prelude::*;
use crate::storage::{FileStorage, Storage, crc32, format};
use std::path::{Path, PathBuf};

/// Checkpoint 数据结构
///
/// 保存恢复点信息，用于快速重启恢复。
#[derive(Debug, Clone)]
pub struct Checkpoint {
    /// 最后有效写入位置
    pub last_valid_position: CheckpointPosition,
    /// 检查点创建时间戳（Unix 时间戳）
    pub timestamp: u64,
    /// 已完成但未同步的段列表
    pub sealed_segments: Vec<u64>,
    /// 检查点版本
    pub version: u32,
}

/// 恢复位置信息
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointPosition {
    /// 段 ID
    pub segment_id: u64,
    /// 段内偏移量
    pub offset: u64,
    /// 最后一条记录的起始位置（用于回滚判断）
    pub last_record_start: u64,
}

impl Default for CheckpointPosition {
    fn default() -> Self {
        Self {
            segment_id: 0,
            offset: 0,
            last_record_start: 0,
        }
    }
}

impl Checkpoint {
    /// 创建新检查点
    pub fn new(position: CheckpointPosition) -> Self {
        Self {
            last_valid_position: position,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            sealed_segments: Vec::new(),
            version: 1,
        }
    }

    /// 添加已密封段
    pub fn with_sealed_segment(mut self, segment_id: u64) -> Self {
        self.sealed_segments.push(segment_id);
        self
    }

    /// 序列化检查点
    ///
    /// 格式：[4B version][8B timestamp][24B position][4B sealed_count][N*8B sealed_segments][4B crc32]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        // version (4 bytes)
        bytes.extend_from_slice(&self.version.to_be_bytes());

        // timestamp (8 bytes)
        bytes.extend_from_slice(&self.timestamp.to_be_bytes());

        // last_valid_position (24 bytes: segment_id + offset + last_record_start)
        bytes.extend_from_slice(&self.last_valid_position.segment_id.to_be_bytes());
        bytes.extend_from_slice(&self.last_valid_position.offset.to_be_bytes());
        bytes.extend_from_slice(&self.last_valid_position.last_record_start.to_be_bytes());

        // sealed_segments count (4 bytes)
        bytes.extend_from_slice(&(self.sealed_segments.len() as u32).to_be_bytes());

        // sealed_segments
        for &seg_id in &self.sealed_segments {
            bytes.extend_from_slice(&seg_id.to_be_bytes());
        }

        // 计算并追加 CRC32 校验和
        let crc = crc32(&bytes);
        bytes.extend_from_slice(&crc.to_be_bytes());

        bytes
    }

    /// 从字节流反序列化
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        // 最小长度：version(4) + timestamp(8) + position(24) + sealed_count(4) + crc(4) = 44
        if bytes.len() < 44 {
            return Err(Error::Generic("Invalid checkpoint data".into()));
        }

        // 验证 CRC32
        let crc_offset = bytes.len() - 4;
        let stored_crc = u32::from_be_bytes([
            bytes[crc_offset],
            bytes[crc_offset + 1],
            bytes[crc_offset + 2],
            bytes[crc_offset + 3],
        ]);
        let computed_crc = crc32(&bytes[..crc_offset]);
        if stored_crc != computed_crc {
            return Err(Error::ChecksumMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        let mut offset = 0;

        // version
        let version = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        // timestamp
        let timestamp = u64::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // last_valid_position
        let segment_id = u64::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        let offset_field = u64::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        let last_record_start = u64::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
            bytes[offset + 4],
            bytes[offset + 5],
            bytes[offset + 6],
            bytes[offset + 7],
        ]);
        offset += 8;

        // sealed_segments count
        let sealed_count = u32::from_be_bytes([
            bytes[offset],
            bytes[offset + 1],
            bytes[offset + 2],
            bytes[offset + 3],
        ]);
        offset += 4;

        // sealed_segments
        let mut sealed_segments = Vec::with_capacity(sealed_count as usize);
        for _ in 0..sealed_count {
            if offset + 8 > bytes.len() {
                return Err(Error::Generic("Invalid checkpoint data".into()));
            }
            let seg_id = u64::from_be_bytes([
                bytes[offset],
                bytes[offset + 1],
                bytes[offset + 2],
                bytes[offset + 3],
                bytes[offset + 4],
                bytes[offset + 5],
                bytes[offset + 6],
                bytes[offset + 7],
            ]);
            offset += 8;
            sealed_segments.push(seg_id);
        }

        Ok(Self {
            last_valid_position: CheckpointPosition {
                segment_id,
                offset: offset_field,
                last_record_start,
            },
            timestamp,
            sealed_segments,
            version,
        })
    }
}

/// 恢复模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// 完整扫描模式 - 扫描所有段，验证每条记录
    FullScan,
    /// 快速恢复模式 - 使用 Checkpoint 跳转到上次有效位置
    Incremental,
    /// 仅验证模式 - 只检查不恢复
    VerifyOnly,
}

/// 恢复结果
#[derive(Debug, Clone)]
pub struct RecoveryResult {
    /// 恢复的记录数
    pub records_recovered: u64,
    /// 跳过的损坏记录数
    pub corrupted_skipped: u64,
    /// 截断的段数
    pub segments_truncated: u64,
    /// 最后有效位置
    pub last_valid_position: CheckpointPosition,
    /// 恢复耗时（毫秒）
    pub duration_ms: u64,
}

/// 恢复管理器
///
/// 负责 WAL 的崩溃恢复和 Checkpoint 管理。
pub struct RecoveryManager {
    /// WAL 数据目录
    dir: PathBuf,
    /// Checkpoint 文件路径
    checkpoint_path: PathBuf,
    /// 恢复模式
    mode: RecoveryMode,
}

impl RecoveryManager {
    /// 创建恢复管理器
    pub fn new<P: AsRef<Path>>(dir: P) -> Self {
        let dir = dir.as_ref().to_path_buf();
        let checkpoint_path = dir.join("checkpoint.data");

        Self {
            dir,
            checkpoint_path,
            mode: RecoveryMode::FullScan,
        }
    }

    /// 设置恢复模式
    pub fn with_mode(mut self, mode: RecoveryMode) -> Self {
        self.mode = mode;
        self
    }

    /// 检查是否存在检查点
    pub async fn has_checkpoint(&self) -> bool {
        tokio::fs::metadata(&self.checkpoint_path).await.is_ok()
    }

    /// 加载检查点
    pub async fn load_checkpoint(&self) -> Result<Option<Checkpoint>> {
        if !self.has_checkpoint().await {
            return Ok(None);
        }

        let data = tokio::fs::read(&self.checkpoint_path).await?;
        let checkpoint = Checkpoint::from_bytes(&data)?;
        Ok(Some(checkpoint))
    }

    /// 保存检查点
    pub async fn save_checkpoint(&self, checkpoint: &Checkpoint) -> Result<()> {
        let data = checkpoint.to_bytes();

        // 使用临时文件保证原子性
        let temp_path = self.checkpoint_path.with_extension("tmp");
        tokio::fs::write(&temp_path, &data).await?;
        tokio::fs::rename(&temp_path, &self.checkpoint_path).await?;

        Ok(())
    }

    /// 创建检查点
    ///
    /// 在指定位置创建检查点，标记最后有效写入位置。
    pub async fn create_checkpoint(
        &self,
        segment_id: u64,
        offset: u64,
        last_record_start: u64,
    ) -> Result<Checkpoint> {
        let position = CheckpointPosition {
            segment_id,
            offset,
            last_record_start,
        };

        let checkpoint = Checkpoint::new(position);
        self.save_checkpoint(&checkpoint).await?;

        Ok(checkpoint)
    }

    /// 删除检查点
    pub async fn delete_checkpoint(&self) -> Result<()> {
        if self.has_checkpoint().await {
            tokio::fs::remove_file(&self.checkpoint_path).await?;
        }
        Ok(())
    }

    /// 验证记录的完整性
    ///
    /// 检查 Magic、长度和CRC32是否有效。
    /// 格式：[4B Magic][4B Length][4B CRC32][Data...]
    ///
    /// 返回值：
    /// - Ok(true): 记录完整且CRC验证通过
    /// - Ok(false): 记录损坏或部分写入
    /// - Err: 读取错误
    pub async fn verify_record(&self, storage: &FileStorage, offset: u64) -> Result<bool> {
        // 读取 Magic (4 bytes)
        let magic_bytes = match storage.read(offset, 4).await {
            Ok(bytes) if bytes.len() == 4 => bytes,
            Ok(_) => return Ok(false),
            Err(e) => return Err(e),
        };
        let magic = u32::from_be_bytes([
            magic_bytes[0],
            magic_bytes[1],
            magic_bytes[2],
            magic_bytes[3],
        ]);
        if magic != format::RECORD_MAGIC {
            return Ok(false); // Magic 不匹配，不是有效记录
        }

        // 读取长度 (4 bytes)
        let length_bytes = match storage.read(offset + 4, 4).await {
            Ok(bytes) if bytes.len() == 4 => bytes,
            Ok(_) => return Ok(false),
            Err(e) => return Err(e),
        };
        let length = u32::from_be_bytes([
            length_bytes[0],
            length_bytes[1],
            length_bytes[2],
            length_bytes[3],
        ]) as u64;

        // 验证长度合理性（最大 64MB）
        if length == 0 || length > format::MAX_RECORD_SIZE {
            return Ok(false);
        }

        // 读取 CRC32 (4 bytes)
        let crc_offset = offset + 8;
        let crc_bytes = match storage.read(crc_offset, 4).await {
            Ok(bytes) if bytes.len() == 4 => bytes,
            Ok(_) => return Ok(false),
            Err(e) => return Err(e),
        };
        let expected_crc =
            u32::from_be_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);

        // 验证数据是否可读
        let data_offset = offset + format::RECORD_HEADER_SIZE;
        let file_size = storage.size().await?;

        if data_offset + length > file_size {
            return Ok(false); // 数据不完整
        }

        // 读取数据并验证 CRC32
        let data = match storage.read(data_offset, length).await {
            Ok(d) => d,
            Err(e) => return Err(e),
        };

        let actual_crc = crc32(&data);
        if actual_crc != expected_crc {
            return Ok(false); // CRC 校验失败
        }

        Ok(true)
    }

    /// 获取段文件路径
    fn segment_path(&self, segment_id: u64) -> PathBuf {
        self.dir.join(format!("{:020}.seg", segment_id))
    }

    /// 在指定位置之后查找下一个记录魔术
    ///
    /// 用于损坏恢复时快速定位下一条有效记录。
    /// 从给定偏移量开始扫描，每次前进固定的步长（4字节）查找魔术。
    ///
    /// # 参数
    /// - storage: 文件存储
    /// - start: 开始扫描的位置
    /// - max_scan: 最大扫描字节数（防止无限扫描）
    ///
    /// # 返回
    /// - Ok(Some(offset)): 找到魔术，返回其位置
    /// - Ok(None): 未找到
    /// - Err: 读取错误
    async fn find_next_magic(
        &self,
        storage: &FileStorage,
        start: u64,
        max_scan: u64,
    ) -> Result<Option<u64>> {
        let file_size = storage.size().await?;
        let mut offset = start;
        let end = (start + max_scan).min(file_size.saturating_sub(4));

        while offset < end {
            let magic_bytes = match storage.read(offset, 4).await {
                Ok(bytes) if bytes.len() == 4 => bytes,
                _ => break,
            };

            let magic = u32::from_be_bytes([
                magic_bytes[0],
                magic_bytes[1],
                magic_bytes[2],
                magic_bytes[3],
            ]);

            if magic == format::RECORD_MAGIC {
                return Ok(Some(offset));
            }

            offset += 4; // 4字节步长扫描
        }

        Ok(None)
    }

    /// 列出所有段文件
    async fn list_segments(&self) -> Result<Vec<u64>> {
        let mut entries = tokio::fs::read_dir(&self.dir).await?;
        let mut segment_ids = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name().to_string_lossy().to_string();

            if name.ends_with(".seg") {
                if let Ok(id) = name.trim_end_matches(".seg").parse::<u64>() {
                    segment_ids.push(id);
                }
            }
        }

        segment_ids.sort();
        Ok(segment_ids)
    }

    /// 执行恢复
    ///
    /// 根据恢复模式扫描段文件，恢复有效数据。
    pub async fn recover(&self) -> Result<RecoveryResult> {
        let start_time = std::time::Instant::now();

        match self.mode {
            RecoveryMode::Incremental => self.recover_incremental().await,
            RecoveryMode::FullScan | RecoveryMode::VerifyOnly => self.recover_full_scan().await,
        }
        .map(|mut result| {
            result.duration_ms = start_time.elapsed().as_millis() as u64;
            result
        })
    }

    /// 增量恢复 - 使用 Checkpoint
    async fn recover_incremental(&self) -> Result<RecoveryResult> {
        let checkpoint = match self.load_checkpoint().await? {
            Some(cp) => cp,
            None => {
                // 没有检查点，执行完整扫描
                return self.recover_full_scan().await;
            }
        };

        // 验证检查点位置是否有效
        let seg_path = self.segment_path(checkpoint.last_valid_position.segment_id);

        let storage = match FileStorage::new(&seg_path).await {
            Ok(s) => s,
            Err(_) => {
                // 段文件不存在，回退到完整扫描
                return self.recover_full_scan().await;
            }
        };

        // 验证检查点位置是否有效
        if !self
            .verify_record(&storage, checkpoint.last_valid_position.offset)
            .await?
        {
            // 检查点无效，完整扫描
            return self.recover_full_scan().await;
        }

        Ok(RecoveryResult {
            records_recovered: 0, // 增量恢复不计数
            corrupted_skipped: 0,
            segments_truncated: 0,
            last_valid_position: checkpoint.last_valid_position,
            duration_ms: 0,
        })
    }

    /// 完整扫描恢复
    async fn recover_full_scan(&self) -> Result<RecoveryResult> {
        let segment_ids = self.list_segments().await?;

        let mut records_recovered = 0u64;
        let mut corrupted_skipped = 0u64;
        let mut segments_truncated = 0u64;
        let mut last_valid_position = CheckpointPosition::default();

        for &segment_id in &segment_ids {
            let seg_path = self.segment_path(segment_id);

            let storage = match FileStorage::new(&seg_path).await {
                Ok(s) => s,
                Err(_) => continue,
            };

            let file_size = storage.size().await?;
            let mut offset = format::SEGMENT_HEADER_SIZE; // 跳过段文件头

            // 扫描段文件中的每条记录
            while offset + format::RECORD_HEADER_SIZE <= file_size {
                match self.verify_record(&storage, offset).await {
                    Ok(true) => {
                        // 读取长度获取下一条记录位置 (magic + length = 8 bytes)
                        let length_bytes = storage.read(offset + 4, 4).await?;
                        let length = u32::from_be_bytes([
                            length_bytes[0],
                            length_bytes[1],
                            length_bytes[2],
                            length_bytes[3],
                        ]) as u64;

                        // 仅验证模式下不恢复
                        if self.mode != RecoveryMode::VerifyOnly {
                            records_recovered += 1;
                        }

                        last_valid_position = CheckpointPosition {
                            segment_id,
                            offset: offset + format::RECORD_HEADER_SIZE + length, // 下一条记录起始位置
                            last_record_start: offset,
                        };

                        offset += format::RECORD_HEADER_SIZE + length;
                    }
                    Ok(false) => {
                        // 记录损坏，查找下一个魔术位置
                        corrupted_skipped += 1;
                        match self
                            .find_next_magic(&storage, offset + 4, 64 * 1024)
                            .await?
                        {
                            Some(next_offset) => {
                                offset = next_offset;
                            }
                            None => {
                                // 找不到更多记录，停止扫描
                                break;
                            }
                        }
                    }
                    Err(_) => {
                        // 读取错误，停止扫描
                        break;
                    }
                }

                // 注：已移除无效保护条件（offset == 0 永远不为真）
                // 循环有 file_size 限制，不会真正无限循环
            }

            // 如果段文件有部分写入但已损坏，截断
            if offset < file_size && self.mode != RecoveryMode::VerifyOnly {
                if let Err(e) = storage.truncate(offset).await {
                    tracing::warn!("Failed to truncate segment {}: {}", segment_id, e);
                } else {
                    segments_truncated += 1;
                }
            }
        }

        // 保存检查点
        if self.mode != RecoveryMode::VerifyOnly {
            if let Err(e) = self
                .create_checkpoint(
                    last_valid_position.segment_id,
                    last_valid_position.offset,
                    last_valid_position.last_record_start,
                )
                .await
            {
                tracing::warn!("Failed to save checkpoint: {}", e);
            }
        }

        Ok(RecoveryResult {
            records_recovered,
            corrupted_skipped,
            segments_truncated,
            last_valid_position,
            duration_ms: 0,
        })
    }

    /// 截断活跃段到指定位置
    ///
    /// 用于处理崩溃时正在写入的段。
    pub async fn truncate_active_segment(&self, segment_id: u64, valid_offset: u64) -> Result<()> {
        let seg_path = self.segment_path(segment_id);
        let storage = FileStorage::new(&seg_path).await?;
        storage.truncate(valid_offset).await?;
        Ok(())
    }

    /// 获取最后检查点位置
    ///
    /// 用于恢复后设置读取位置。
    pub async fn get_recovery_position(&self) -> Result<Option<CheckpointPosition>> {
        match self.load_checkpoint().await? {
            Some(cp) => Ok(Some(cp.last_valid_position)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_checkpoint_serialization() {
        let position = CheckpointPosition {
            segment_id: 1,
            offset: 100,
            last_record_start: 92,
        };

        let checkpoint = Checkpoint::new(position);
        let bytes = checkpoint.to_bytes();
        let restored = Checkpoint::from_bytes(&bytes).unwrap();

        assert_eq!(restored.last_valid_position.segment_id, 1);
        assert_eq!(restored.last_valid_position.offset, 100);
        assert_eq!(restored.last_valid_position.last_record_start, 92);
    }

    #[tokio::test]
    async fn test_checkpoint_with_sealed_segments() {
        let position = CheckpointPosition::default();
        let checkpoint = Checkpoint::new(position)
            .with_sealed_segment(1)
            .with_sealed_segment(2);

        let bytes = checkpoint.to_bytes();
        let restored = Checkpoint::from_bytes(&bytes).unwrap();

        assert_eq!(restored.sealed_segments, vec![1, 2]);
    }

    #[tokio::test]
    async fn test_recovery_manager_checkpoint() {
        let temp_dir = tempdir().unwrap();

        let manager = RecoveryManager::new(temp_dir.path());

        // 初始无检查点
        assert!(!manager.has_checkpoint().await);

        // 创建检查点
        let checkpoint = manager.create_checkpoint(1, 100, 92).await.unwrap();

        assert!(manager.has_checkpoint().await);
        assert_eq!(checkpoint.last_valid_position.segment_id, 1);
        assert_eq!(checkpoint.last_valid_position.offset, 100);

        // 加载检查点
        let loaded = manager.load_checkpoint().await.unwrap().unwrap();
        assert_eq!(loaded.last_valid_position.offset, 100);

        // 删除检查点
        manager.delete_checkpoint().await.unwrap();
        assert!(!manager.has_checkpoint().await);
    }

    #[tokio::test]
    async fn test_verify_record() {
        use crate::storage::crc32;
        use crate::storage::format;

        let temp_dir = tempdir().unwrap();
        let manager = RecoveryManager::new(temp_dir.path());

        // 创建测试存储
        let storage = FileStorage::new(temp_dir.path().join("test.seg"))
            .await
            .unwrap();

        // 写入有效记录: [4B Magic][4B Length][4B CRC32][Data]
        let data = b"hello world";
        let data_crc = crc32(data);
        let magic_bytes = format::RECORD_MAGIC.to_be_bytes();
        let length_bytes = (data.len() as u32).to_be_bytes();
        let crc_bytes = data_crc.to_be_bytes();
        storage.append(&magic_bytes).await.unwrap();
        storage.append(&length_bytes).await.unwrap();
        storage.append(&crc_bytes).await.unwrap();
        storage.append(data).await.unwrap();

        // 验证记录 (从偏移量0开始，即记录开始位置)
        let valid = manager.verify_record(&storage, 0).await.unwrap();
        assert!(valid);

        // 验证损坏记录 (偏移量4处是长度，不是有效的记录头)
        let valid = manager.verify_record(&storage, 4).await.unwrap();
        assert!(!valid);
    }
}
