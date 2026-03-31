//! 配置模块
//!
//! 定义 Easy WAL 的配置选项

/// 持久化模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceMode {
    /// 立即持久化：每次写入都调用 fsync
    Immediate,

    /// 批量持久化：累积一定量后调用 fsync
    Batch,

    /// 手动持久化：用户手动调用 flush
    Manual,
}

impl Default for PersistenceMode {
    fn default() -> Self {
        PersistenceMode::Immediate
    }
}

/// WAL 配置
#[derive(Debug, Clone)]
pub struct Config {
    /// 段大小（字节）
    segment_size: usize,

    /// 持久化模式
    persistence_mode: PersistenceMode,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            // 默认段大小：1GB
            segment_size: 1024 * 1024 * 1024,
            persistence_mode: PersistenceMode::default(),
        }
    }
}

impl Config {
    /// 创建新的配置实例
    pub fn new() -> Self {
        Config::default()
    }

    /// 设置段大小（builder 模式）
    ///
    /// # 参数
    /// - `size`: 段大小（字节）
    ///
    /// # 返回
    /// 返回修改后的配置实例
    pub fn with_segment_size(mut self, size: usize) -> Self {
        self.segment_size = size;
        self
    }

    /// 获取段大小
    pub fn segment_size(&self) -> usize {
        self.segment_size
    }

    /// 设置持久化模式（builder 模式）
    ///
    /// # 参数
    /// - `mode`: 持久化模式
    ///
    /// # 返回
    /// 返回修改后的配置实例
    pub fn with_persistence_mode(mut self, mode: PersistenceMode) -> Self {
        self.persistence_mode = mode;
        self
    }

    /// 获取持久化模式
    pub fn persistence_mode(&self) -> PersistenceMode {
        self.persistence_mode
    }

    /// 验证配置的有效性
    ///
    /// # 返回
    /// 如果配置有效返回 Ok(())，否则返回错误消息
    pub fn validate(&self) -> crate::Result<()> {
        if self.segment_size == 0 {
            return Err(crate::Error::Config {
                message: "segment size must be greater than 0".to_string(),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.segment_size(), 1024 * 1024 * 1024);
        assert_eq!(config.persistence_mode(), PersistenceMode::Immediate);
    }

    #[test]
    fn test_custom_config() {
        let config = Config::new()
            .with_segment_size(1024)
            .with_persistence_mode(PersistenceMode::Batch);

        assert_eq!(config.segment_size(), 1024);
        assert_eq!(config.persistence_mode(), PersistenceMode::Batch);
    }

    #[test]
    fn test_config_validation() {
        let valid_config = Config::new().with_segment_size(1024);
        assert!(valid_config.validate().is_ok());

        let invalid_config = Config::new().with_segment_size(0);
        assert!(invalid_config.validate().is_err());
    }
}
