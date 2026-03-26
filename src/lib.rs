//! Easy WAL - 一个教学级 WAL（Write-Ahead Logging）系统
//!
//! 这个项目旨在构建一个生产级的 WAL 系统，同时作为 Rust 教学项目。
//!
//! # 架构设计
//!
//! 项目采用四层架构：
//! - Layer 1: 存储层 (`storage`) - 底层存储抽象
//! - Layer 2: 组件层 - 核心功能组件（未来实现）
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
//!
//! # 当前状态
//!
//! Phase 1: 存储层重构 (进行中)
//! - ✅ Storage trait 设计
//! - ✅ FileStorage 实现
//! - ✅ MemoryStorage 实现
//! - ⬜ 测试完善

mod error;
mod prelude;
pub mod storage;

// 重导出常用类型，方便用户使用
pub use prelude::{Error, Result};
pub use storage::{FileStorage, Location, MemoryStorage, Storage, StorageStats};
