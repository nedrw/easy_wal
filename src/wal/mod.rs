//! WAL 模块 - 四层架构的协调层和 API 层
//!
//! # 架构设计
//! - Layer 2: 组件层 - LogWriter, LogReader, SegmentManager
//! - Layer 3: 协调层 - WriteCoordinator, ReadCoordinator, RecoveryManager
//! - Layer 4: API 层 - WalManager, WalBuilder

mod coordinators;
mod recovery;
mod sync_strategy;
mod wal_manager;

pub use coordinators::{ReadCoordinator, WriteCoordinator};
pub use recovery::{Checkpoint, CheckpointPosition, RecoveryManager, RecoveryMode, RecoveryResult};
pub use sync_strategy::{SyncMode, SyncStats, SyncStrategy};
pub use wal_manager::{Record, WalBuilder, WalConfig, WalManager};

// 重新导出组件层类型，方便使用
pub use crate::storage::{
    LogReader, LogReaderConfig, LogWriter, LogWriterConfig, ReadPosition, SegmentMeta,
    WritePosition,
};
