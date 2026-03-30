//! Easy WAL - 一个教学级 WAL（Write-Ahead Logging）系统
//!
//! 这个项目旨在构建一个生产级的 WAL 系统，同时作为 Rust 教学项目。
//!
//! # 架构设计
//!
//! 项目采用四层架构：
//! - Layer 1: 存储层 (`storage`) - 底层存储抽象
//! - Layer 2: 组件层 - 核心功能组件（当前实现）
//! - Layer 3: 协调层 - 生命周期管理（未来实现）
//! - Layer 4: API 层 - 用户接口（未来实现）
//!
//! # 教学目标
//!
//! 每个 Layer 都有明确的教学价值：
//! - Layer 1: Rust trait 设计、文件 I/O、错误处理
//! - Layer 2: 组件化设计、状态管理、并发控制
//! - Layer 3: 协调器模式、恢复机制、生命周期管理
//! - Layer 4: API 设计、Builder 模式、用户体验

mod error;
mod prelude;
pub mod storage;
pub mod wal;

// 重导出常用类型，方便用户使用
pub use prelude::{Error, Result};

// 存储层导出
pub use storage::{
    Crc32, FileStorage, Location, LogWriter, LogWriterConfig, MemoryStorage, SegmentConfig,
    SegmentManager, SegmentMeta, SegmentStats, Storage, StorageStats, WritePosition, crc32,
    verify_crc32,
};

// WAL 层导出（协调层 + API 层）
pub use wal::{
    Checkpoint, CheckpointPosition, CommitConfig, CommitCoordinator, CommitStats, ReadCoordinator,
    RecoveryManager, RecoveryMode, RecoveryResult, SyncPolicy, WalBuilder, WalConfig, WalManager,
    WriterHandle,
};
