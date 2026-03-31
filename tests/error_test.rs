//! 错误类型测试
//!
//! 测试 Easy WAL 的核心错误特性

use easy_wal::{Error, Result};
use std::io;

#[test]
fn test_from_io_error() {
    // 测试从 std::io::Error 转换（重要的 trait 实现）
    let io_err = io::Error::new(io::ErrorKind::PermissionDenied, "permission denied");
    let error: Error = io_err.into();

    assert!(matches!(error, Error::Io(_)));
    assert!(error.to_string().contains("permission denied"));
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
        Error::Io(io::Error::new(io::ErrorKind::NotFound, "file not found")),
        Error::Corruption {
            offset: 1024,
            reason: "CRC check failed".to_string(),
        },
        Error::SegmentNotFound { offset: 2048 },
        Error::Config {
            message: "invalid config".to_string(),
        },
        Error::Closed,
    ];

    // 验证每个错误变体的 Display 输出
    for error in &errors {
        let display_str = error.to_string();
        assert!(!display_str.is_empty(), "Error display output is empty");
    }

    // 验证具体的错误信息包含关键字
    assert!(errors[0].to_string().contains("IO error"));
    assert!(errors[1].to_string().contains("Data corruption"));
    assert!(errors[2].to_string().contains("Segment not found"));
    assert!(errors[3].to_string().contains("Configuration error"));
    assert!(errors[4].to_string().contains("closed"));
}
