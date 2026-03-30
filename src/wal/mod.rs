//! WAL 模块 - 四层架构的协调层和 API 层
//!
//! # 架构设计
//! - Layer 2: 组件层 - LogWriter, LogReader, SegmentManager
//! - Layer 3: 协调层 - WriteCoordinator, ReadCoordinator, RecoveryManager
//! - Layer 4: API 层 - WalManager, WalBuilder

mod commit_coordinator;
mod coordinators;
mod recovery;
mod segment_coordinator;
mod sync_strategy;
mod wal_manager;
mod writer_handle;

pub use commit_coordinator::{
    CommitConfig, CommitCoordinator, CommitStats, SequenceNumber, WriteBatch, WriteMode, WriterId,
    WriterMeta, WriterStats,
};
pub use coordinators::{ReadCoordinator, WriteCoordinator};
pub use recovery::{Checkpoint, CheckpointPosition, RecoveryManager, RecoveryMode, RecoveryResult};
pub use segment_coordinator::{ExtendedSegmentStats, RotationConfig, SegmentCoordinator};
pub use sync_strategy::{SyncContext, SyncMode, SyncStats, SyncStrategy};
pub use wal_manager::{Record, WalBuilder, WalConfig, WalManager};
pub use writer_handle::WriterHandle;

// 重新导出组件层类型，方便使用
pub use crate::storage::{
    LogReader, LogReaderConfig, LogWriter, LogWriterConfig, ReadPosition, SegmentMeta,
    WritePosition,
};
