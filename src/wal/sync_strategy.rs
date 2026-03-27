//! 同步策略模块
//!
//! 提供不同的数据同步策略，用于在性能和数据安全性之间取得平衡。

/// 同步模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// 不同步 - 依赖操作系统缓冲区
    /// 风险：崩溃可能丢失最后几秒的数据
    None,

    /// 每次写入后同步
    /// 最安全但性能最低
    /// 典型延迟：5-15ms (HDD), 0.5-2ms (SSD)
    FsyncOnWrite,

    /// 定期同步
    /// 最多丢失 interval 时间的数据
    /// 性能最高但风险也最高
    Periodic {
        /// 同步间隔（毫秒）
        interval_ms: u64,
    },

    /// 批量写入后同步
    /// 在批量导入或突发流量时，每 N 次写入同步一次
    /// 在性能和安全之间取得平衡
    Batch {
        /// 批量大小（写入次数）
        batch_size: u64,
    },
}

/// 同步统计
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    /// 总同步次数
    pub sync_count: u64,
    /// 总同步耗时（毫秒）
    pub sync_time_ms: u64,
    /// 最后同步时间戳
    pub last_sync_at: Option<std::time::Instant>,
}

impl SyncStats {
    /// 创建新的同步统计
    pub fn new() -> Self {
        Self::default()
    }

    /// 记录一次同步
    pub fn record(&mut self, duration_ms: u64) {
        self.sync_count += 1;
        self.sync_time_ms += duration_ms;
        self.last_sync_at = Some(std::time::Instant::now());
    }

    /// 获取平均同步延迟（毫秒）
    pub fn avg_sync_latency(&self) -> u64 {
        if self.sync_count == 0 {
            0
        } else {
            self.sync_time_ms / self.sync_count
        }
    }
}

/// 同步上下文
///
/// 跟踪同步状态，决定何时执行同步操作。
/// 由 WriteCoordinator 持有和使用。
pub struct SyncContext {
    mode: SyncMode,
    /// 批量写入计数器
    batch_counter: u64,
    /// 最后同步时间（用于周期同步）
    last_sync_time: std::time::Instant,
}

impl SyncContext {
    /// 创建新的同步上下文
    pub fn new(mode: SyncMode) -> Self {
        let now = std::time::Instant::now();
        Self {
            mode,
            batch_counter: 0,
            last_sync_time: now,
        }
    }

    /// 创建批量同步策略
    pub fn batch(batch_size: u64) -> Self {
        Self::new(SyncMode::Batch { batch_size })
    }

    /// 创建周期同步策略
    pub fn periodic(interval_ms: u64) -> Self {
        Self::new(SyncMode::Periodic { interval_ms })
    }

    /// 获取同步模式
    pub fn mode(&self) -> SyncMode {
        self.mode
    }

    /// 设置同步模式（运行时切换）
    ///
    /// 切换模式时会重置内部状态：
    /// - 批量计数器清零
    /// - 同步计时器重置
    /// - 保留历史统计信息（由 SyncStats 维护）
    pub fn set_mode(&mut self, mode: SyncMode) {
        self.mode = mode;
        self.batch_counter = 0;
        self.last_sync_time = std::time::Instant::now();
    }

    /// 单条写入后调用，返回是否需要 sync
    pub fn on_write(&mut self) -> bool {
        match self.mode {
            SyncMode::None => false,

            SyncMode::FsyncOnWrite => true,

            SyncMode::Batch { batch_size } => {
                self.batch_counter += 1;
                if self.batch_counter >= batch_size {
                    self.batch_counter = 0;
                    true
                } else {
                    false
                }
            }

            SyncMode::Periodic { .. } => false, // 不在 on_write 触发
        }
    }

    /// 批量写入后调用，返回是否需要 sync
    pub fn on_batch(&mut self, count: u64) -> bool {
        match self.mode {
            SyncMode::None => false,

            SyncMode::FsyncOnWrite => true,

            SyncMode::Batch { batch_size } => {
                self.batch_counter += count;
                if self.batch_counter >= batch_size {
                    self.batch_counter %= batch_size;
                    true
                } else {
                    false
                }
            }

            SyncMode::Periodic { .. } => false, // 不在 on_batch 触发
        }
    }

    /// Periodic 定时检查（由外部定时任务调用）
    pub fn should_periodic_sync(&self) -> bool {
        match self.mode {
            SyncMode::Periodic { interval_ms } => {
                self.last_sync_time.elapsed().as_millis() as u64 >= interval_ms
            }
            _ => false,
        }
    }

    /// 记录同步完成
    pub fn on_synced(&mut self) {
        self.last_sync_time = std::time::Instant::now();
    }

    /// 强制重置状态
    pub fn reset(&mut self) {
        self.batch_counter = 0;
        self.last_sync_time = std::time::Instant::now();
    }
}

impl Default for SyncContext {
    fn default() -> Self {
        Self::new(SyncMode::None)
    }
}

impl std::fmt::Debug for SyncContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncContext")
            .field("mode", &self.mode)
            .field("batch_counter", &self.batch_counter)
            .finish()
    }
}

// 保留 SyncStrategy 作为 SyncContext 的类型别名，用于向后兼容
pub type SyncStrategy = SyncContext;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_mode_none() {
        let context = SyncContext::new(SyncMode::None);
        assert_eq!(context.mode(), SyncMode::None);
    }

    #[tokio::test]
    async fn test_fsync_on_write() {
        let mut context = SyncContext::new(SyncMode::FsyncOnWrite);

        // 每次写入都应该触发同步
        let should_sync = context.on_write();
        assert!(should_sync);

        context.on_synced();
    }

    #[tokio::test]
    async fn test_batch_sync() {
        let mut context = SyncContext::batch(3);

        // 前两次写入不触发同步
        assert!(!context.on_write());
        assert!(!context.on_write());

        // 第三次触发
        let should_sync = context.on_write();
        assert!(should_sync);

        context.on_synced();
        assert_eq!(context.batch_counter, 0);
    }

    #[tokio::test]
    async fn test_batch_sync_with_on_batch() {
        let mut context = SyncContext::batch(10);

        // 批量写入 3 条
        assert!(!context.on_batch(3));
        assert_eq!(context.batch_counter, 3);

        // 批量写入 5 条
        assert!(!context.on_batch(5));
        assert_eq!(context.batch_counter, 8);

        // 批量写入 4 条，总共 12 条，超过 batch_size=10
        let should_sync = context.on_batch(4);
        assert!(should_sync);
        assert_eq!(context.batch_counter, 2); // 12 % 10 = 2

        context.on_synced();
    }

    #[tokio::test]
    async fn test_periodic_sync() {
        let mut context = SyncContext::periodic(100); // 100ms interval

        // 刚创建时不应该触发
        assert!(!context.should_periodic_sync());

        // 手动设置 last_sync_time 为很久以前
        context.last_sync_time = std::time::Instant::now() - std::time::Duration::from_millis(150);
        assert!(context.should_periodic_sync());

        // 同步后重置时间
        context.on_synced();
        assert!(!context.should_periodic_sync());
    }

    #[tokio::test]
    async fn test_periodic_does_not_trigger_on_write() {
        let mut context = SyncContext::periodic(100);

        // Periodic 模式下 on_write 不应该触发同步
        assert!(!context.on_write());
        assert!(!context.on_write());
        assert!(!context.on_write());
    }

    #[tokio::test]
    async fn test_reset() {
        let mut context = SyncContext::batch(10);

        context.on_write();
        context.on_write();
        assert_eq!(context.batch_counter, 2);

        context.reset();
        assert_eq!(context.batch_counter, 0);
    }

    #[test]
    fn test_sync_stats() {
        let mut stats = SyncStats::new();

        stats.record(5);
        assert_eq!(stats.sync_count, 1);
        assert_eq!(stats.sync_time_ms, 5);

        stats.record(10);
        assert_eq!(stats.sync_count, 2);
        assert_eq!(stats.sync_time_ms, 15);
        assert_eq!(stats.avg_sync_latency(), 7); // 15 / 2 = 7
    }
}
