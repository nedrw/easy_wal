//! Easy WAL - 基于 Kafka 模式的 WAL 库
//!
//! 设计理念：
//! - 简单直接：借鉴 Kafka Log 的设计，避免过度分层
//! - 状态一致：读写共享同一个段对象，消除状态同步问题
//! - 高性能：内置预读缓冲区和批量写入优化
//!
//! 参考：
//! - Kafka Log 的段管理设计
//! - etcd/raft WAL 的简洁架构
//! - RocksDB WAL 的读写分离模式

mod config;
mod error;
mod segment;
mod wal;

// 重新导出公共类型
pub use config::{Config, PersistenceMode};
pub use error::{Error, Result};
pub use segment::LogSegment;
pub use wal::Wal;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_exports() {
        // 确保所有公共类型都正确导出
        let config = Config::default();
        assert!(config.segment_size() > 0);

        let persistence_mode = PersistenceMode::Immediate;
        assert_eq!(persistence_mode, PersistenceMode::Immediate);
    }
}
