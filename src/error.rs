//! 错误类型定义
//!
//! 定义 Easy WAL 使用的错误类型

use std::fmt;
use std::io;

/// Easy WAL 的错误类型
#[derive(Debug)]
pub enum Error {
    /// IO 错误
    Io(io::Error),

    /// 数据损坏错误
    Corruption {
        /// 损坏数据的偏移量
        offset: u64,
        /// 损坏原因
        reason: String,
    },

    /// 段未找到错误
    SegmentNotFound {
        /// 请求的偏移量
        offset: u64,
    },

    /// 配置错误
    Config {
        /// 错误消息
        message: String,
    },

    /// WAL 已关闭
    Closed,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(err) => write!(f, "IO error: {}", err),
            Error::Corruption { offset, reason } => {
                write!(f, "Data corruption at offset {}: {}", offset, reason)
            }
            Error::SegmentNotFound { offset } => {
                write!(f, "Segment not found for offset: {}", offset)
            }
            Error::Config { message } => {
                write!(f, "Configuration error: {}", message)
            }
            Error::Closed => {
                write!(f, "WAL has been closed")
            }
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

/// Easy WAL 的 Result 类型别名
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_send_sync() {
        // 确保错误类型实现了 Send + Sync
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Error>();
    }

    #[test]
    fn test_error_display() {
        let err = Error::Corruption {
            offset: 1024,
            reason: "CRC check failed".to_string(),
        };
        assert!(err.to_string().contains("1024"));
        assert!(err.to_string().contains("CRC check failed"));
    }
}
