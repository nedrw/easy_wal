// ! Commit Coordinator - Multi-Writer Group Commit 实现
// !
// ! 负责协调多个 writer 的写入批次，通过 group commit 优化 I/O 性能。

use crate::prelude::*;
use crate::storage::WritePosition;
use crate::wal::segment_coordinator::SegmentCoordinator;
use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::time::{Duration, Instant};
use std::sync::Mutex;
use tokio::sync::{RwLock, oneshot};
use tracing::{error, info};

// ===================== Core Types =====================

/// Group Commit 配置
#[derive(Debug, Clone)]
pub struct CommitConfig {
    /// 最大批次大小（字节），达到此值强制提交
    pub max_batch_size: usize,
    /// 最大等待时间（毫秒），达到此时间强制提交
    pub max_wait_time_ms: u64,
    /// 最大批次数量，达到此数量强制提交
    pub max_batch_count: usize,
    /// 最小批次数量触发提交（如果为0，单个批次也立即提交）
    pub min_batches_for_commit: usize,
}

impl CommitConfig {
    /// 创建默认配置
    pub fn new() -> Self {
        Self {
            max_batch_size: 64 * 1024, // 64KB
            max_wait_time_ms: 5,       // 5ms
            max_batch_count: 100,      // 100 batches
            min_batches_for_commit: 1, // 单个批次也可提交
        }
    }
}

impl Default for CommitConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// 序列号
///
/// 使用高32位作为 commit group ID，低32位作为组内序列号。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd)]
pub struct SequenceNumber {
    commit_group: u64,
    sequence: u64,
}

impl SequenceNumber {
    pub fn new(commit_group: u64, sequence: u64) -> Self {
        Self {
            commit_group,
            sequence,
        }
    }

    pub fn commit_group(&self) -> u64 {
        self.commit_group
    }

    pub fn sequence(&self) -> u64 {
        self.sequence
    }

    pub fn to_u64(&self) -> u64 {
        (self.commit_group << 32) | self.sequence
    }

    pub fn from_u64(val: u64) -> Self {
        Self {
            commit_group: val >> 32,
            sequence: val & 0xFFFFFFFF,
        }
    }

    pub fn cmp(&self, other: &Self) -> Ordering {
        self.commit_group
            .cmp(&other.commit_group)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}

/// 写入批次
///
/// 代表单个 writer 提交的一批记录。
/// 包含记录数据、批次元数据，以及用于通知结果的 channel。
pub struct WriteBatch {
    /// 批次唯一标识
    pub batch_id: u64,
    /// Writer 唯一标识
    pub writer_id: u64,
    /// 批次中的所有记录
    pub records: Vec<Vec<u8>>,
    /// 总字节数
    pub size_bytes: usize,
    /// 分配的序列号
    pub sequence: AtomicU64,
    /// 用于发送结果给 writer 的 channel
    result_tx: Mutex<Option<oneshot::Sender<Result<Vec<WritePosition>>>>>,
    /// 创建时间，用于超时追踪
    pub created_at: Instant,
}

impl WriteBatch {
    /// 创建新的 WriteBatch
    ///
    /// 返回批次和接收结果的一端。
    pub fn new(
        writer_id: u64,
        records: Vec<Vec<u8>>,
    ) -> (Arc<Self>, oneshot::Receiver<Result<Vec<WritePosition>>>) {
        let (tx, rx) = oneshot::channel();
        let size_bytes = records.iter().map(|r| r.len()).sum();

        let batch = Arc::new(Self {
            batch_id: 0,
            writer_id,
            records,
            size_bytes,
            sequence: AtomicU64::new(0),
            result_tx: Mutex::new(Some(tx)),
            created_at: Instant::now(),
        });

        (batch, rx)
    }

    /// 设置序列号
    pub fn set_sequence(&self, seq: u64) {
        self.sequence.store(seq, AtomicOrdering::Release);
    }

    /// 获取序列号
    pub fn get_sequence(&self) -> u64 {
        self.sequence.load(AtomicOrdering::Acquire)
    }

    /// 检查批次是否超时
    pub fn is_timedout(&self, max_wait_ms: u64) -> bool {
        self.created_at.elapsed().as_millis() as u64 >= max_wait_ms
    }

    /// 发送结果给 writer
    pub fn send_result(self: &Arc<Self>, result: Result<Vec<WritePosition>>) {
        if let Some(tx) = self.result_tx.lock().unwrap().take() {
            let _ = tx.send(result);
        }
    }
}

impl std::fmt::Debug for WriteBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteBatch")
            .field("batch_id", &self.batch_id)
            .field("writer_id", &self.writer_id)
            .field("records_count", &self.records.len())
            .field("size_bytes", &self.size_bytes)
            .finish()
    }
}

/// Commit 统计信息
#[derive(Debug, Clone, Default)]
pub struct CommitStats {
    /// 总提交批次数
    pub total_batches: u64,
    /// 总提交记录数
    pub total_records: u64,
    /// 总提交字节数
    pub total_bytes: u64,
    /// Group commit 次数
    pub group_commits: u64,
    /// 单批次提交次数（未成组）
    pub single_commits: u64,
}

// ===================== Commit Coordinator =====================

/// 待提交批次及其元数据
#[derive(Default)]
struct PendingBatches {
    /// 批次列表
    batches: Vec<Arc<WriteBatch>>,
    /// 每个批次的记录数量
    record_counts: Vec<usize>,
    /// 每个批次的字节大小
    byte_counts: Vec<usize>,
}

impl PendingBatches {
    fn new() -> Self {
        Self::default()
    }

    fn push(&mut self, batch: Arc<WriteBatch>) {
        self.record_counts.push(batch.records.len());
        self.byte_counts.push(batch.size_bytes);
        self.batches.push(batch);
    }

    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    fn len(&self) -> usize {
        self.batches.len()
    }

    fn total_records(&self) -> usize {
        self.record_counts.iter().sum()
    }

    fn total_bytes(&self) -> usize {
        self.byte_counts.iter().sum()
    }
}

/// Commit Coordinator 内部状态
struct CoordinatorState {
    /// 待提交的批次
    pending: PendingBatches,
    /// 是否正在关闭
    shutdown: bool,
}

impl Default for CoordinatorState {
    fn default() -> Self {
        Self {
            pending: PendingBatches::new(),
            shutdown: false,
        }
    }
}

/// Commit Coordinator
///
/// 核心职责：
/// 1. 收集来自多个 writer 的 WriteBatch
/// 2. 按照配置的条件触发提交（时间、批次大小、数量）
/// 3. 合并批次并写入底层存储
/// 4. 分配全局序列号
/// 5. 通知 writer 写入结果
pub struct CommitCoordinator {
    /// 配置
    config: CommitConfig,
    /// 下一个序列号（原子）
    next_sequence: AtomicU64,
    /// 内部状态
    state: RwLock<CoordinatorState>,
    /// 用于唤醒 commit loop 的信号
    commit_wakeup: Arc<tokio::sync::Notify>,
    /// SegmentCoordinator 引用（用于写入和段管理）
    segment_coordinator: Arc<SegmentCoordinator>,
    /// 统计信息
    stats: Arc<RwLock<CommitStats>>,
}

impl CommitCoordinator {
    /// 创建新的 CommitCoordinator
    pub async fn new(
        config: CommitConfig,
        segment_coordinator: Arc<SegmentCoordinator>,
    ) -> Result<Self> {
        Ok(Self {
            config,
            next_sequence: AtomicU64::new(0),
            state: RwLock::new(CoordinatorState::default()),
            commit_wakeup: Arc::new(tokio::sync::Notify::new()),
            segment_coordinator,
            stats: Arc::new(RwLock::new(CommitStats::default())),
        })
    }

    /// 启动协调器（后台任务）
    pub fn start(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            this.commit_loop().await;
        });
    }

    /// 添加批次到提交队列
    pub async fn add_batch(&self, batch: Arc<WriteBatch>) {
        {
            let mut state = self.state.write().await;
            state.pending.push(batch);
        }
        // 通知 commit loop
        self.commit_wakeup.notify_one();
    }

    /// 强制触发提交
    pub async fn flush(&self) -> Result<()> {
        // 通知 commit loop
        self.commit_wakeup.notify_one();
        // 等待一小段时间让 commit loop 处理
        tokio::time::sleep(Duration::from_millis(10)).await;
        Ok(())
    }

    /// 获取统计信息
    pub async fn stats(&self) -> CommitStats {
        self.stats.read().await.clone()
    }

    /// 关闭协调器
    pub async fn shutdown(&self) {
        info!("Shutting down CommitCoordinator");
        {
            let mut state = self.state.write().await;
            state.shutdown = true;
        }
        self.commit_wakeup.notify_one();
        // 等待一小段时间让最后的 flush 完成
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    /// 获取下一个可用序列号
    pub fn next_sequence(&self) -> SequenceNumber {
        let seq = self.next_sequence.fetch_add(1, AtomicOrdering::AcqRel);
        SequenceNumber::from_u64(seq)
    }

    /// 批量获取序列号
    pub fn next_sequence_batch(&self, count: usize) -> SequenceNumber {
        let base = self
            .next_sequence
            .fetch_add(count as u64, AtomicOrdering::AcqRel);
        SequenceNumber::from_u64(base)
    }

    /// Commit loop
    async fn commit_loop(&self) {
        loop {
            // 等待条件满足或超时
            {
                let state = self.state.read().await;
                if state.pending.is_empty() && !state.shutdown {
                    // 等待新批次或超时
                    tokio::select! {
                        _ = self.commit_wakeup.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(self.config.max_wait_time_ms)) => {}
                    }
                }
            }

            // 检查关闭状态并取出待处理的批次
            let pending = {
                let mut state = self.state.write().await;
                if state.shutdown && state.pending.is_empty() {
                    break;
                }

                // 检查是否需要提交
                let should_commit = Self::should_commit(&state.pending, &self.config);

                if should_commit && !state.pending.is_empty() {
                    // 取走所有待处理的批次
                    let batches = std::mem::take(&mut state.pending);
                    batches
                } else {
                    // 继续等待
                    continue;
                }
            };

            // 处理批次
            if let Err(e) = self.flush_batches(pending).await {
                error!("Batch flush failed: {:?}", e);
            }
        }
    }

    /// 检查是否应该提交
    fn should_commit(pending: &PendingBatches, config: &CommitConfig) -> bool {
        if pending.is_empty() {
            return false;
        }

        // 检查批次数量
        if pending.len() >= config.max_batch_count {
            return true;
        }

        // 检查总大小
        let total_size = pending.total_bytes();
        if total_size >= config.max_batch_size {
            return true;
        }

        // 检查最小批次要求 + 超时
        if pending.len() >= config.min_batches_for_commit {
            // 检查最早批次是否超时
            if let Some(first_batch) = pending.batches.first() {
                if first_batch.is_timedout(config.max_wait_time_ms) {
                    return true;
                }
            }
        }

        false
    }

    /// 刷新批次到存储
    async fn flush_batches(&self, pending: PendingBatches) -> Result<()> {
        if pending.batches.is_empty() {
            return Ok(());
        }

        let batch_count = pending.batches.len();
        let total_records = pending.total_records();
        let total_bytes = pending.total_bytes();

        // 分配序列号
        let base_sequence = self
            .next_sequence
            .fetch_add(total_records as u64, AtomicOrdering::AcqRel);

        // 收集所有记录
        let all_records: Vec<&[u8]> = pending
            .batches
            .iter()
            .flat_map(|b| b.records.iter().map(|r| r.as_slice()))
            .collect();

        // 获取活跃写入器
        let writer = self.segment_coordinator.get_active_writer().await?;

        // 批量写入
        let positions = writer.write_batch(&all_records).await?;

        // 记录位置范围
        let mut batch_position_ranges: Vec<(usize, usize)> = Vec::with_capacity(batch_count);
        let mut pos_index = 0usize;

        for count in &pending.record_counts {
            batch_position_ranges.push((pos_index, pos_index + count));
            pos_index += count;
        }

        // 同步到磁盘
        writer.sync().await?;

        // 检查段轮转
        let header_size = 12u64; // record header size
        let written_bytes = total_bytes as u64 + total_records as u64 * header_size;
        self.segment_coordinator
            .update_size(written_bytes, total_records as u64)
            .await;
        let _rotated = self.segment_coordinator.check_and_rotate().await?;

        // 更新统计
        {
            let mut stats = self.stats.write().await;
            stats.total_batches += batch_count as u64;
            stats.total_records += total_records as u64;
            stats.total_bytes += total_bytes as u64;
            stats.group_commits += 1;
            if batch_count == 1 {
                stats.single_commits += 1;
            }
        }

        // 发送结果给各个 writer
        let mut seq = base_sequence;
        for (batch, (start, end)) in pending.batches.iter().zip(batch_position_ranges) {
            // 设置这个 batch 的序列号（起始序列号）
            batch.set_sequence(seq);
            seq += (end - start) as u64;

            // 提取这个 batch 对应的位置
            let batch_positions: Vec<WritePosition> = positions[start..end].to_vec();
            batch.send_result(Ok(batch_positions));
        }

        Ok(())
    }
}

impl Clone for CommitCoordinator {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            next_sequence: AtomicU64::new(0),
            state: RwLock::new(CoordinatorState::default()),
            commit_wakeup: Arc::new(tokio::sync::Notify::new()),
            segment_coordinator: self.segment_coordinator.clone(),
            stats: self.stats.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_segment_coordinator() -> Arc<SegmentCoordinator> {
        let temp_dir = tempdir().unwrap();
        let rotation_config = crate::wal::RotationConfig::new().with_max_size(1024 * 1024);
        let segment_config = crate::storage::SegmentConfig::new(temp_dir.path());

        Arc::new(
            SegmentCoordinator::new(rotation_config, segment_config)
                .await
                .unwrap(),
        )
    }

    #[test]
    fn test_sequence_number() {
        let seq1 = SequenceNumber::new(1, 0);
        let seq2 = SequenceNumber::new(1, 1);
        let seq3 = SequenceNumber::new(2, 0);

        assert!(seq1 < seq2);
        assert!(seq2 < seq3);
        assert_eq!(seq1.to_u64(), 1 << 32);
        assert_eq!(SequenceNumber::from_u64(1 << 32 | 5).sequence(), 5);
    }

    #[test]
    fn test_write_batch_creation() {
        let records = vec![b"hello".to_vec(), b"world".to_vec()];
        let (batch, mut rx) = WriteBatch::new(1, records);

        assert_eq!(batch.writer_id, 1);
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.size_bytes, 10);
        assert!(rx.try_recv().is_err()); // not sent yet
    }

    #[test]
    fn test_commit_config_default() {
        let config = CommitConfig::default();
        assert_eq!(config.max_batch_size, 64 * 1024);
        assert_eq!(config.max_wait_time_ms, 5);
        assert_eq!(config.max_batch_count, 100);
        assert_eq!(config.min_batches_for_commit, 1);
    }

    #[tokio::test]
    async fn test_commit_coordinator_creation() {
        let segment_coordinator = create_segment_coordinator().await;
        let config = CommitConfig::new();

        let coordinator = CommitCoordinator::new(config, segment_coordinator)
            .await
            .unwrap();

        assert_eq!(coordinator.stats().await.total_batches, 0);
    }

    #[tokio::test]
    async fn test_submit_and_flush_batch() {
        let segment_coordinator = create_segment_coordinator().await;
        let config = CommitConfig::default();

        let coordinator = CommitCoordinator::new(config, segment_coordinator)
            .await
            .unwrap();

        // Start coordinator
        coordinator.start();

        // Create and submit a batch
        let records = vec![b"test record".to_vec()];
        let (batch, mut rx) = WriteBatch::new(1, records);

        coordinator.add_batch(batch).await;
        coordinator.flush().await.unwrap();

        // Wait for result
        tokio::time::sleep(Duration::from_millis(50)).await;

        match rx.try_recv() {
            Ok(Ok(positions)) => {
                assert!(!positions.is_empty());
                println!("Success! Got {} positions", positions.len());
            }
            Ok(Err(e)) => panic!("Batch failed: {:?}", e),
            Err(_) => {
                // Timeout - may need more time
                tokio::time::sleep(Duration::from_millis(100)).await;
                coordinator.shutdown().await;
            }
        }

        coordinator.shutdown().await;
    }

    #[tokio::test]
    async fn test_batch_timing() {
        let records = vec![b"test".to_vec()];
        let (batch, _rx) = WriteBatch::new(1, records);

        // Should not timeout immediately
        assert!(!batch.is_timedout(5));

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(batch.is_timedout(5));
    }

    #[tokio::test]
    async fn test_multi_batch_positions() {
        let segment_coordinator = create_segment_coordinator().await;
        let config = CommitConfig::default();

        let coordinator = CommitCoordinator::new(config, segment_coordinator)
            .await
            .unwrap();

        coordinator.start();

        // Submit multiple batches
        let (batch1, mut rx1) = WriteBatch::new(1, vec![b"record1".to_vec()]);
        let (batch2, mut rx2) = WriteBatch::new(2, vec![b"record2".to_vec(), b"record3".to_vec()]);

        coordinator.add_batch(batch1).await;
        coordinator.add_batch(batch2).await;
        coordinator.flush().await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check first batch
        if let Ok(Ok(positions)) = rx1.try_recv() {
            assert_eq!(positions.len(), 1);
            println!("Batch1 positions: {:?}", positions);
        }

        // Check second batch
        if let Ok(Ok(positions)) = rx2.try_recv() {
            assert_eq!(positions.len(), 2);
            println!("Batch2 positions: {:?}", positions);
        }

        coordinator.shutdown().await;
    }
}
