//! 错误类型测试
//!
//! 测试 Easy WAL 的错误类型定义

use easy_wal::{Error, Result};
use std::io;

#[test]
fn test_io_error_creation() {
    // 测试创建 IO 错误
    let io_err = io::Error::new(io::ErrorKind::NotFound, "file not found");
    let error = Error::Io(io_err);

    assert!(matches!(error, Error::Io(_)));
    assert!(error.to_string().contains("file not found"));
}

#[test]
fn test_corruption_error_creation() {
    // 测试创建数据损坏错误
    let error = Error::Corruption {
        offset: 1024,
        reason: "CRC check failed".to_string(),
    };

    assert!(matches!(error, Error::Corruption { offset: 1024, .. }));
    assert!(error.to_string().contains("CRC check failed"));
    assert!(error.to_string().contains("1024"));
}

#[test]
fn test_segment_error_creation() {
    // 测试创建段错误
    let error = Error::SegmentNotFound { offset: 2048 };

    assert!(matches!(error, Error::SegmentNotFound { offset: 2048 }));
    assert!(error.to_string().contains("2048"));
}

#[test]
fn test_config_error_creation() {
    // 测试创建配置错误
    let error = Error::Config {
        message: "segment size must be positive".to_string(),
    };

    assert!(matches!(error, Error::Config { .. }));
    assert!(error.to_string().contains("segment size must be positive"));
}

#[test]
fn test_closed_error_creation() {
    // 测试创建已关闭错误
    let error = Error::Closed;

    assert!(matches!(error, Error::Closed));
    assert!(error.to_string().contains("closed"));
}

#[test]
fn test_from_io_error() {
    // 测试从 std::io::Error 转换
    let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");
    let error: Error = io_err.into();

    assert!(matches!(error, Error::Io(_)));
}

#[test]
fn test_result_type() {
    // 测试 Result 类型别名
    fn returns_error() -> Result<()> {
        Err(Error::Config {
            message: "test error".to_string(),
        })
    }

    let result = returns_error();
    assert!(result.is_err());

    let error = result.unwrap_err();
    assert!(matches!(error, Error::Config { .. }));
}

#[test]
fn test_error_send_sync() {
    // 测试错误类型是否实现 Send + Sync（对于多线程环境很重要）
    fn assert_send_sync<T: Send + Sync>() {}

    assert_send_sync::<Error>();
}

#[test]
fn test_error_display() {
    // 测试所有错误类型的 Display 实现
    let errors = vec![
        Error::Io(io::Error::new(io::ErrorKind::NotFound, "test")),
        Error::Corruption {
            offset: 0,
            reason: "test".to_string(),
        },
        Error::SegmentNotFound { offset: 0 },
        Error::Config {
            message: "test".to_string(),
        },
        Error::Closed,
    ];

    for error in errors {
        // 确保每个错误都有有意义的 Display 输出
        let msg = error.to_string();
        assert!(!msg.is_empty());
    }
}
