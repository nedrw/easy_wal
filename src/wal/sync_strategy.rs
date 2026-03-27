//! 同步策略模块
//!
//! 提供不同的数据同步策略，用于在性能和数据安全性之间取得平衡。

use tokio::sync::RwLock;

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
}

/// 同步统计
#[derive(Debug, Clone, Default)]
pub struct SyncStats {
    /// 总同步次数
    pub sync_count: u64,
    /// 总同步字节数
    pub synced_bytes: u64,
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
    pub fn record(&mut self, bytes: u64, duration_ms: u64) {
        self.sync_count += 1;
        self.synced_bytes += bytes;
        self.sync_time_ms += duration_ms;
        self.last_sync_at = Some(std::time::Instant::now());
    }

    /// 获取平均同步大小（字节）
    pub fn avg_sync_size(&self) -> u64 {
        if self.sync_count == 0 {
            0
        } else {
            self.synced_bytes / self.sync_count
        }
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

/// 同步策略上下文
///
/// 跟踪同步状态，决定何时执行同步操作。
pub struct SyncStrategy {
    mode: SyncMode,
    /// 待同步字节数
    pending_bytes: u64,
    /// 最后同步时间（用于周期同步）
    last_sync_time: std::time::Instant,
    /// 统计信息
    stats: RwLock<SyncStats>,
}

impl SyncStrategy {
    /// 创建新的同步策略
    pub fn new(mode: SyncMode) -> Self {
        let now = std::time::Instant::now();
        Self {
            mode,
            pending_bytes: 0,
            last_sync_time: now,
            stats: RwLock::new(SyncStats::new()),
        }
    }

    /// 创建周期同步策略
    pub fn periodic(interval_ms: u64) -> Self {
        Self::new(SyncMode::Periodic { interval_ms })
    }

    /// 获取同步模式
    pub fn mode(&self) -> SyncMode {
        self.mode
    }

    /// 获取统计信息
    pub async fn stats(&self) -> SyncStats {
        self.stats.read().await.clone()
    }

    /// 通知写入完成
    ///
    /// # 返回
    /// - `Some(bytes)` 表示需要同步，返回待同步字节数
    /// - `None` 表示不需要同步
    pub async fn on_write(&mut self, bytes: u64) -> Option<u64> {
        self.pending_bytes += bytes;

        match self.mode {
            SyncMode::None => None,

            SyncMode::FsyncOnWrite => Some(self.pending_bytes),

            SyncMode::Periodic { interval_ms } => {
                let elapsed = self.last_sync_time.elapsed().as_millis() as u64;
                if elapsed >= interval_ms {
                    self.last_sync_time = std::time::Instant::now();
                    Some(self.pending_bytes)
                } else {
                    None
                }
            }
        }
    }

    /// 记录同步完成
    pub async fn on_synced(&mut self, bytes: u64, duration_ms: u64) {
        self.pending_bytes = self.pending_bytes.saturating_sub(bytes);

        let mut stats = self.stats.write().await;
        stats.record(bytes, duration_ms);
    }

    /// 强制重置状态
    pub async fn reset(&mut self) {
        self.pending_bytes = 0;
        self.last_sync_time = std::time::Instant::now();
    }

    /// 获取待同步字节数
    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes
    }
}

impl Default for SyncStrategy {
    fn default() -> Self {
        Self::new(SyncMode::None)
    }
}

impl std::fmt::Debug for SyncStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncStrategy")
            .field("mode", &self.mode)
            .field("pending_bytes", &self.pending_bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sync_mode_none() {
        let strategy = SyncStrategy::new(SyncMode::None);
        assert_eq!(strategy.mode(), SyncMode::None);
        assert_eq!(strategy.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn test_fsync_on_write() {
        let mut strategy = SyncStrategy::new(SyncMode::FsyncOnWrite);

        // 每次写入都应该触发同步
        let should_sync = strategy.on_write(100).await;
        assert!(should_sync.is_some());
        assert_eq!(should_sync.unwrap(), 100);

        strategy.on_synced(100, 5).await;
        assert_eq!(strategy.pending_bytes(), 0);
    }

    #[tokio::test]
    async fn test_periodic_sync() {
        let mut strategy = SyncStrategy::periodic(100); // 100ms interval

        // 第一次写入（时间太短，不触发）
        assert!(strategy.on_write(100).await.is_none());

        // 模拟时间流逝（需要实际等待或 mock）
        // 这里只测试初始化
        assert_eq!(strategy.pending_bytes(), 100);
    }

    #[tokio::test]
    async fn test_stats() {
        let mut strategy = SyncStrategy::new(SyncMode::FsyncOnWrite);

        strategy.on_write(100).await;
        strategy.on_synced(100, 5).await;

        let stats = strategy.stats().await;
        assert_eq!(stats.sync_count, 1);
        assert_eq!(stats.synced_bytes, 100);
        assert_eq!(stats.sync_time_ms, 5);
    }

    #[tokio::test]
    async fn test_reset() {
        let mut strategy = SyncStrategy::new(SyncMode::FsyncOnWrite);

        strategy.on_write(100).await;
        assert_eq!(strategy.pending_bytes(), 100);

        strategy.reset().await;
        assert_eq!(strategy.pending_bytes(), 0);
    }
}
