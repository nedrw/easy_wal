//! 统计模块
//!
//! 提供 WAL 操作的统计信息，可通过 `stats` feature flag 控制开关
//!
//! # Feature Flag
//!
//! - `stats` (默认启用): 启用统计功能，记录 WAL 操作的统计信息
//! - 禁用 `stats` feature: 完全零开销，所有统计相关代码会被编译器优化掉

#[cfg(feature = "stats")]
use std::sync::atomic::{AtomicU64, Ordering};

/// WAL 统计信息（快照）
///
/// 用于对外暴露当前的统计状态
#[cfg(feature = "stats")]
#[derive(Debug, Clone, Copy)]
pub struct WalStats {
    /// 总记录数
    pub total_records: u64,

    /// 总字节数
    pub total_bytes: u64,

    /// 写入次数
    pub write_count: u64,

    /// 读取次数
    pub read_count: u64,

    /// 刷新次数
    pub flush_count: u64,

    /// 当前段数量
    pub segment_count: u64,
}

/// WAL 统计信息（空实现，用于禁用统计时）
#[cfg(not(feature = "stats"))]
#[derive(Debug, Clone, Copy)]
pub struct WalStats;

/// 内部统计结构
///
/// 使用原子计数器记录各种操作统计
#[cfg(feature = "stats")]
pub(crate) struct Stats {
    /// 总记录数
    total_records: AtomicU64,

    /// 总字节数
    total_bytes: AtomicU64,

    /// 写入次数
    write_count: AtomicU64,

    /// 读取次数
    read_count: AtomicU64,

    /// 刷新次数
    flush_count: AtomicU64,

    /// 当前段数量
    segment_count: AtomicU64,
}

/// 内部统计结构（空实现，用于禁用统计时）
///
/// 零大小类型（ZST），完全零开销
#[cfg(not(feature = "stats"))]
pub(crate) struct Stats;

#[cfg(feature = "stats")]
impl Stats {
    /// 创建新的统计实例
    pub(crate) fn new() -> Self {
        Stats {
            total_records: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
            read_count: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
            segment_count: AtomicU64::new(1), // 初始有1个段
        }
    }

    /// 记录一次写入操作
    #[inline]
    pub(crate) fn record_write(&self, bytes: u64) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        self.total_records.fetch_add(1, Ordering::Relaxed);
        self.total_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// 记录一次读取操作
    #[inline]
    pub(crate) fn record_read(&self) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
    }

    /// 记录一次刷新操作
    #[inline]
    pub(crate) fn record_flush(&self) {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
    }

    /// 设置段数量
    #[inline]
    pub(crate) fn set_segment_count(&self, count: u64) {
        self.segment_count.store(count, Ordering::Relaxed);
    }

    /// 增加段数量
    #[inline]
    pub(crate) fn increment_segment_count(&self) {
        self.segment_count.fetch_add(1, Ordering::Relaxed);
    }

    /// 减少段数量
    #[inline]
    pub(crate) fn decrement_segment_count(&self) {
        self.segment_count.fetch_sub(1, Ordering::Relaxed);
    }

    /// 获取当前统计快照
    pub(crate) fn snapshot(&self) -> WalStats {
        WalStats {
            total_records: self.total_records.load(Ordering::Relaxed),
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            write_count: self.write_count.load(Ordering::Relaxed),
            read_count: self.read_count.load(Ordering::Relaxed),
            flush_count: self.flush_count.load(Ordering::Relaxed),
            segment_count: self.segment_count.load(Ordering::Relaxed),
        }
    }
}

#[cfg(not(feature = "stats"))]
impl Stats {
    /// 创建新的统计实例（空实现）
    #[inline]
    pub(crate) fn new() -> Self {
        Stats
    }

    /// 记录一次写入操作（空实现）
    #[inline]
    pub(crate) fn record_write(&self, _bytes: u64) {
        // 完全零开销，编译器会优化掉
    }

    /// 记录一次读取操作（空实现）
    #[inline]
    pub(crate) fn record_read(&self) {
        // 完全零开销，编译器会优化掉
    }

    /// 记录一次刷新操作（空实现）
    #[inline]
    pub(crate) fn record_flush(&self) {
        // 完全零开销，编译器会优化掉
    }

    /// 设置段数量（空实现）
    #[inline]
    pub(crate) fn set_segment_count(&self, _count: u64) {
        // 完全零开销，编译器会优化掉
    }

    /// 增加段数量（空实现）
    #[inline]
    pub(crate) fn increment_segment_count(&self) {
        // 完全零开销，编译器会优化掉
    }

    /// 减少段数量（空实现）
    #[inline]
    pub(crate) fn decrement_segment_count(&self) {
        // 完全零开销，编译器会优化掉
    }

    /// 获取当前统计快照（空实现）
    #[inline]
    pub(crate) fn snapshot(&self) -> WalStats {
        WalStats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg(feature = "stats")]
    fn test_stats_recording() {
        let stats = Stats::new();

        // 初始状态
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.write_count, 0);
        assert_eq!(snapshot.read_count, 0);
        assert_eq!(snapshot.total_records, 0);
        assert_eq!(snapshot.total_bytes, 0);

        // 记录写入
        stats.record_write(100);
        stats.record_write(200);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.write_count, 2);
        assert_eq!(snapshot.total_records, 2);
        assert_eq!(snapshot.total_bytes, 300);

        // 记录读取
        stats.record_read();
        stats.record_read();
        stats.record_read();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.read_count, 3);

        // 记录刷新
        stats.record_flush();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.flush_count, 1);
    }

    #[test]
    #[cfg(feature = "stats")]
    fn test_segment_count() {
        let stats = Stats::new();

        // 初始段数量为1
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.segment_count, 1);

        // 增加段
        stats.increment_segment_count();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.segment_count, 2);

        // 减少段
        stats.decrement_segment_count();
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.segment_count, 1);

        // 设置段数量
        stats.set_segment_count(5);
        let snapshot = stats.snapshot();
        assert_eq!(snapshot.segment_count, 5);
    }

    #[test]
    #[cfg(not(feature = "stats"))]
    fn test_stats_disabled() {
        let stats = Stats::new();

        // 所有操作都是空实现，不应该崩溃
        stats.record_write(100);
        stats.record_read();
        stats.record_flush();
        stats.increment_segment_count();
        stats.decrement_segment_count();
        stats.set_segment_count(5);

        let _snapshot = stats.snapshot();
    }
}
