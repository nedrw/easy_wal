//! 错误类型定义
//!
//! 统一管理所有模块的错误类型，便于错误处理和传播。

#[derive(thiserror::Error, Debug)]
pub enum Error {
    // ============ 通用错误 ============
    #[error("Generic: {0}")]
    Generic(String),

    #[error("End of file")]
    Eof,

    #[error(transparent)]
    IO(#[from] std::io::Error),

    // ============ 存储层错误 ============
    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Storage closed")]
    StorageClosed,

    #[error("Storage read failed: offset={offset}, size={size}, error={error}")]
    StorageRead {
        offset: u64,
        size: u64,
        error: String,
    },

    #[error("Storage write failed: offset={offset}, size={size}, error={error}")]
    StorageWrite {
        offset: u64,
        size: u64,
        error: String,
    },

    // ============ 校验和/CRC 错误 ============
    #[error("Checksum mismatch: expected={expected:02x}, actual={actual:02x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Invalid checksum data: {0}")]
    InvalidChecksumData(String),

    // ============ 段管理错误 ============
    #[error("Segment error: {0}")]
    Segment(String),

    #[error("Segment not found: id={id}")]
    SegmentNotFound { id: u64 },

    #[error("Segment file corrupted: id={id}, error={error}")]
    SegmentCorrupted { id: u64, error: String },

    #[error("Max segment size exceeded: {0} bytes")]
    MaxSegmentSizeExceeded(u64),

    #[error("Segment creation failed: {0}")]
    SegmentCreationFailed(String),

    // ============ WAL 相关错误 ============
    #[error("WAL error: {0}")]
    Wal(String),

    #[error("WAL closed")]
    WalClosed,

    #[error("Invalid WAL position: segment={segment_id}, offset={offset}")]
    InvalidWalPosition { segment_id: u64, offset: u64 },

    #[error("WAL truncated during recovery")]
    WalTruncated,

    // ============ 恢复相关错误 ============
    #[error("Recovery error: {0}")]
    Recovery(String),

    #[error("Recovery failed: {0}")]
    RecoveryFailed(String),

    #[error("Checkpoint error: {0}")]
    Checkpoint(String),

    #[error("Checkpoint not found")]
    CheckpointNotFound,

    #[error("Invalid checkpoint data: {0}")]
    InvalidCheckpointData(String),

    #[error("Recovery mode conflict: cannot use {mode1} and {mode2} simultaneously")]
    RecoveryModeConflict { mode1: String, mode2: String },

    // ============ 同步错误 ============
    #[error("Sync error: {0}")]
    Sync(String),

    #[error("Sync timeout after {0}ms")]
    SyncTimeout(u64),

    // ============ 配置错误 ============
    #[error("Config error: {0}")]
    Config(String),

    #[error("Invalid config value: {field}={value}, reason={reason}")]
    InvalidConfig {
        field: String,
        value: String,
        reason: String,
    },
}
