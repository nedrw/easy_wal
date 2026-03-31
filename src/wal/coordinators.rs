//! WAL 协调器 - 读写协调组件
//!
//! # 教学价值
//! - 学习协调器模式
//! - 学习读写分离
//! - 学习缓冲优化

use crate::prelude::*;
use crate::storage::{LogReader, ReadPosition, SegmentMeta, WritePosition};
use crate::wal::{SegmentCoordinator, SyncContext, SyncMode, SyncStats};
use std::sync::Arc;
use tokio::sync::RwLock;

/// 最大单条记录大小 (64MB)
const MAX_RECORD_SIZE: u64 = 64 * 1024 * 1024;

// ============================================================
// 写入协调器
// ============================================================

/// 同步报告
///
/// 包含同步操作的执行结果信息。
#[derive(Debug, Clone)]
pub struct SyncReport {
    /// 同步耗时（毫秒）
    pub duration_ms: u64,
    /// 是否成功
    pub success: bool,
    /// 错误信息（仅在 success 为 false 时有值）
    pub error: Option<String>,
}

/// 写入协调器
///
/// 协调写入操作，提供：
/// - 同步策略执行
/// - 批量写入优化
/// - 与 RecoveryManager 协作
pub struct WriteCoordinator {
    segment_coordinator: Arc<SegmentCoordinator>,
    sync_context: RwLock<SyncContext>,
    /// 同步统计信息
    sync_stats: RwLock<SyncStats>,
}

impl WriteCoordinator {
    /// 创建写入协调器
    pub fn new(segment_coordinator: Arc<SegmentCoordinator>, sync_mode: SyncMode) -> Self {
        Self {
            segment_coordinator,
            sync_context: RwLock::new(SyncContext::new(sync_mode)),
            sync_stats: RwLock::new(SyncStats::new()),
        }
    }

    /// 创建批量同步策略的协调器
    pub fn with_batch(segment_coordinator: Arc<SegmentCoordinator>, batch_size: u64) -> Self {
        Self::new(segment_coordinator, SyncMode::Batch { batch_size })
    }

    /// 创建周期同步策略的协调器
    pub fn with_periodic(segment_coordinator: Arc<SegmentCoordinator>, interval_ms: u64) -> Self {
        Self::new(segment_coordinator, SyncMode::Periodic { interval_ms })
    }

    /// 获取段协调器
    pub fn segment_coordinator(&self) -> Arc<SegmentCoordinator> {
        self.segment_coordinator.clone()
    }

    /// 获取同步模式
    pub async fn sync_mode(&self) -> SyncMode {
        let ctx = self.sync_context.read().await;
        ctx.mode()
    }

    /// 设置同步模式（运行时修改）
    ///
    /// 允许在运行时切换同步策略。注意：
    /// - 会重置批量计数器
    /// - 会重置定时器
    /// - 不会清除历史统计信息
    pub async fn set_sync_mode(&self, mode: SyncMode) {
        let mut ctx = self.sync_context.write().await;
        ctx.set_mode(mode);
    }

    /// 获取同步统计信息
    pub async fn sync_stats(&self) -> SyncStats {
        self.sync_stats.read().await.clone()
    }

    /// 写入单条数据
    pub async fn write(&self, data: &[u8]) -> Result<WritePosition> {
        if data.len() as u64 > MAX_RECORD_SIZE {
            return Err(Error::Generic(format!(
                "Record too large: {} > {}",
                data.len(),
                MAX_RECORD_SIZE
            )));
        }

        // 1. 从协调器获取活跃段（Kafka 模式）
        let segment = self.segment_coordinator.get_active_segment().await?;

        // 2. 执行写入
        let pos = segment.append(data).await?;

        // 3. 更新段大小（通知协调器）
        let bytes_written = crate::storage::format::RECORD_HEADER_SIZE + data.len() as u64;
        self.segment_coordinator.update_size(bytes_written, 1).await;

        // 4. 检查是否需要轮转（由协调器决策）
        self.segment_coordinator.check_and_rotate().await?;

        // 5. 根据策略决定是否同步
        let should_sync = {
            let mut ctx = self.sync_context.write().await;
            ctx.on_write()
        };

        if should_sync {
            self.do_sync().await?;
        }

        Ok(pos)
    }

    /// 批量写入
    ///
    /// 对于 Batch 模式，批量写入会累加计数，达到阈值后触发同步。
    pub async fn write_batch(&self, data_list: &[&[u8]]) -> Result<Vec<WritePosition>> {
        if data_list.is_empty() {
            return Ok(Vec::new());
        }

        // 检查记录大小
        for data in data_list {
            if data.len() as u64 > MAX_RECORD_SIZE {
                return Err(Error::Generic(format!(
                    "Record too large: {} > {}",
                    data.len(),
                    MAX_RECORD_SIZE
                )));
            }
        }

        // 1. 从协调器获取活跃段（Kafka 模式）
        let segment = self.segment_coordinator.get_active_segment().await?;

        // 2. 执行批量写入
        let positions = segment.append_batch(data_list).await?;

        // 3. 更新段大小（通知协调器）
        let total_bytes = data_list
            .iter()
            .map(|d| crate::storage::format::RECORD_HEADER_SIZE + d.len() as u64)
            .sum();
        self.segment_coordinator
            .update_size(total_bytes, positions.len() as u64)
            .await;

        // 4. 检查是否需要轮转（由协调器决策）
        self.segment_coordinator.check_and_rotate().await?;

        // 5. 根据策略决定是否同步
        let should_sync = {
            let mut ctx = self.sync_context.write().await;
            ctx.on_batch(positions.len() as u64)
        };

        if should_sync {
            self.do_sync().await?;
        }

        Ok(positions)
    }

    /// 检查并执行周期性同步
    ///
    /// 此方法应该由外部定时任务调用，用于 Periodic 同步模式。
    /// 对于其他模式，此方法不做任何操作。
    pub async fn check_periodic_sync(&self) -> Result<()> {
        let should_sync = {
            let ctx = self.sync_context.read().await;
            ctx.should_periodic_sync()
        };

        if should_sync {
            self.do_sync().await?;
        }

        Ok(())
    }

    /// 强制同步
    ///
    /// 立即执行 fsync，忽略同步策略。
    /// 返回同步报告，包含耗时和执行结果。
    pub async fn sync(&self) -> Result<SyncReport> {
        self.do_sync().await
    }

    /// 执行实际的同步操作
    async fn do_sync(&self) -> Result<SyncReport> {
        let start = std::time::Instant::now();

        // 获取活跃段并同步（Kafka 模式）
        let segment = self.segment_coordinator.get_active_segment().await?;
        let result = segment.sync().await;

        let duration_ms = start.elapsed().as_millis() as u64;

        match result {
            Ok(()) => {
                // 更新策略状态
                {
                    let mut ctx = self.sync_context.write().await;
                    ctx.on_synced();
                }

                // 更新统计信息
                {
                    let mut stats = self.sync_stats.write().await;
                    stats.record(duration_ms);
                }

                Ok(SyncReport {
                    duration_ms,
                    success: true,
                    error: None,
                })
            }
            Err(e) => {
                tracing::error!("Sync failed after {}ms: {}", duration_ms, e);
                Err(Error::Sync(e.to_string()))
            }
        }
    }

    /// 强制轮转段
    pub async fn rotate(&self) -> Result<(u64, std::path::PathBuf)> {
        self.segment_coordinator.force_rotate().await
    }

    /// 获取活跃段 ID
    pub async fn active_segment_id(&self) -> u64 {
        self.segment_coordinator.active_segment_id().await
    }

    /// 获取所有段信息
    pub async fn segments(&self) -> Vec<SegmentMeta> {
        self.segment_coordinator.segments().await
    }

    /// 关闭写入协调器
    pub async fn close(&self) -> Result<()> {
        self.sync().await?;
        // 获取活跃段并同步（Kafka 模式）
        let segment = self.segment_coordinator.get_active_segment().await?;
        segment.sync().await
    }
}

// ============================================================
// 读取协调器
// ============================================================

/// 读取位置（方案 C+ 核心数据结构）
///
/// 与 LogReader 的 ReadPosition 分离，避免预读缓冲区导致的位置跟踪问题。
/// 方案 C+：消费位置和预读位置分离，确保 position() 始终返回准确的消费位置。
#[derive(Debug, Clone, Copy)]
struct ReadCoordPosition {
    /// 段 ID
    segment_id: u64,
    /// 段内偏移量
    offset: u64,
}

impl ReadCoordPosition {
    fn new(segment_id: u64, offset: u64) -> Self {
        Self { segment_id, offset }
    }
}

/// 读取协调器（方案 C+）
///
/// 协调读取操作，提供：
/// - 读取缓冲（预读优化）
/// - 并发读取控制
/// - 独立位置跟踪（不依赖 LogReader）
///
/// # 方案 C+ 架构
/// - `consume_position`: 消费位置，用户通过 `position()` 看到的是这个
/// - `read_ahead_position`: 预读位置，`fill_buffer()` 使用这个进行 IO
/// 两者分离确保跨段场景下位置跟踪的准确性。
pub struct ReadCoordinator {
    reader: Arc<RwLock<LogReader>>,
    /// 预读缓冲区
    read_ahead_buffer: Arc<RwLock<ReadAheadBuffer>>,
    /// 预读大小
    read_ahead_size: usize,
    /// 消费位置：已解析并返回给用户的记录位置
    consume_position: Arc<RwLock<ReadCoordPosition>>,
    /// 预读位置：下一次 fill_buffer 的 IO 起始位置
    read_ahead_position: Arc<RwLock<ReadCoordPosition>>,
}

/// 预读缓冲区
struct ReadAheadBuffer {
    /// 缓冲区数据
    data: Vec<u8>,
    /// 当前读取位置
    pos: usize,
    /// 是否已耗尽
    exhausted: bool,
    /// 是否有残留的不完整数据（无法解析出完整记录）
    has_incomplete: bool,
}

impl ReadAheadBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            pos: 0,
            exhausted: false,
            has_incomplete: false,
        }
    }

    /// 清空缓冲区
    fn clear(&mut self) {
        self.data.clear();
        self.pos = 0;
        self.exhausted = false;
        self.has_incomplete = false;
    }

    /// 检查缓冲区是否有可用的完整数据
    /// 如果有残留的不完整数据，返回 false 以强制重新填充
    fn has_data(&self) -> bool {
        // 有不完整的残留数据时，返回 false 强制重新填充
        if self.has_incomplete {
            return false;
        }
        self.pos < self.data.len()
    }

    /// 从缓冲区读取数据
    fn read(&mut self) -> Option<Vec<u8>> {
        if !self.has_data() {
            return None;
        }

        // 读取 Magic (4 bytes)
        if self.pos + 4 > self.data.len() {
            // 数据不足以读取 magic，标记为不完整，触发重新填充
            self.has_incomplete = true;
            return None;
        }
        let magic_bytes: [u8; 4] = self.data[self.pos..self.pos + 4].try_into().unwrap();
        let magic = u32::from_be_bytes(magic_bytes);

        // 验证 Magic
        if magic != crate::storage::format::RECORD_MAGIC {
            // magic 不匹配，可能数据在边界被分割，标记为不完整以触发重新填充
            self.has_incomplete = true;
            return None;
        }

        // 读取长度 (4 bytes)
        if self.pos + 8 > self.data.len() {
            // 数据不足以读取长度，标记为不完整，触发重新填充
            self.has_incomplete = true;
            return None;
        }
        let length_bytes: [u8; 4] = self.data[self.pos + 4..self.pos + 8].try_into().unwrap();
        let length = u32::from_be_bytes(length_bytes) as usize;

        // 验证数据完整性
        let record_size = 4 + 4 + 4 + length; // magic + length + crc + data
        if self.pos + record_size > self.data.len() {
            // 标记有残留不完整数据，下次 has_data() 将返回 false
            self.has_incomplete = true;
            return None;
        }

        // 提取数据（跳过 magic 4B + length 4B + crc 4B）
        let data_start = self.pos + 12;
        let data = self.data[data_start..data_start + length].to_vec();

        self.pos += record_size;
        Some(data)
    }

    /// 填充缓冲区（保留未解析的数据）
    ///
    /// 当有不完整的记录数据时，保留这些数据并追加新数据，避免数据丢失。
    /// 返回保留的数据长度，用于调整 read_ahead_position。
    ///
    /// # 边界处理
    /// - 限制保留数据的最大长度，避免保留过多数据导致 read_ahead_position 倒退
    /// - 当新数据较少时（如段末尾），自动调整保留长度
    fn fill(&mut self, data: Vec<u8>) -> usize {
        const MAX_PRESERVED_LEN: usize = 4 * 1024; // 最多保留 4KB

        // 如果有不完整的数据，保留它并追加新数据
        if self.has_incomplete && self.pos < self.data.len() {
            let remaining_len = self.data.len() - self.pos;

            // 限制保留长度，避免保留过多数据
            let preserved_len = remaining_len.min(MAX_PRESERVED_LEN);

            // 从缓冲区末尾取 preserved_len 字节
            let preserved_start = self.data.len() - preserved_len;
            let remaining = self.data[preserved_start..].to_vec();

            self.data = remaining;
            self.data.extend_from_slice(&data);
            self.pos = 0;
            self.exhausted = false;
            self.has_incomplete = false;

            preserved_len
        } else {
            // 没有不完整数据，直接替换
            self.data = data;
            self.pos = 0;
            self.exhausted = false;
            self.has_incomplete = false;
            0
        }
    }
}

impl ReadCoordinator {
    /// 创建读取协调器（方案 C+）
    ///
    /// 初始化消费位置和预读位置为相同值。
    pub async fn new(reader: Arc<RwLock<LogReader>>) -> Self {
        // 从 LogReader 获取初始位置
        let initial_pos = reader.read().await.position().await;

        // 方案 C+：消费位置和预读位置分离
        let consume_position = Arc::new(RwLock::new(ReadCoordPosition::new(
            initial_pos.segment_id,
            initial_pos.offset,
        )));
        let read_ahead_position = Arc::new(RwLock::new(ReadCoordPosition::new(
            initial_pos.segment_id,
            initial_pos.offset,
        )));

        Self {
            reader,
            read_ahead_buffer: Arc::new(RwLock::new(ReadAheadBuffer::new(64 * 1024))),
            read_ahead_size: 64 * 1024,
            consume_position,
            read_ahead_position,
        }
    }

    /// 设置预读大小
    ///
    /// 方案 C+：预读功能已恢复，消费位置和预读位置分离确保跨段正确性。
    pub fn with_read_ahead(mut self, size: usize) -> Self {
        self.read_ahead_size = size;
        self.read_ahead_buffer = Arc::new(RwLock::new(ReadAheadBuffer::new(size)));
        self
    }

    /// 尝试从预读缓冲区读取
    /// 成功读取记录时更新消费位置（consume_position）。
    /// 注意：先读取数据释放 buffer 锁，再更新 position，避免死锁。
    async fn read_from_buffer(&self) -> Option<Vec<u8>> {
        eprintln!("[READ_BUF] start");
        // 第一步：从缓冲区读取数据（持有 buffer 锁）
        let data_with_size = {
            let mut buffer = self.read_ahead_buffer.write().await;
            eprintln!(
                "[READ_BUF] buffer pos={}, len={}",
                buffer.pos,
                buffer.data.len()
            );
            if let Some(data) = buffer.read() {
                let record_size = crate::storage::format::RECORD_HEADER_SIZE + data.len() as u64;
                eprintln!("[READ_BUF] read {} bytes", data.len());
                Some((data, record_size))
            } else {
                eprintln!("[READ_BUF] no data from buffer");
                None
            }
        };

        // 第二步：更新消费位置（不持有 buffer 锁，避免死锁）
        // 方案 C+：只更新 consume_position，不影响 read_ahead_position
        if let Some((data, record_size)) = data_with_size {
            let mut pos = self.consume_position.write().await;
            let old_offset = pos.offset;
            pos.offset += record_size;
            eprintln!(
                "[READ_BUF] updated consume_position: {} -> {}",
                old_offset, pos.offset
            );
            return Some(data);
        }

        eprintln!("[READ_BUF] returning None");
        None
    }

    /// 填充预读缓冲区（方案 C+）
    ///
    /// 使用 read_ahead_position 计算读取位置，读取后更新 read_ahead_position。
    /// consume_position 由 read_from_buffer() 更新，两者分离确保跨段正确性。
    async fn fill_buffer(&self) -> Result<()> {
        eprintln!("[FILL] start");
        let mut buffer = self.read_ahead_buffer.write().await;

        // 只有当缓冲区完全为空时才填充
        if buffer.has_data() {
            eprintln!("[FILL] buffer has data, skipping");
            return Ok(());
        }

        // 方案 C+：使用 read_ahead_position 计算读取位置
        let (segment_id, offset) = {
            let pos = self.read_ahead_position.read().await;
            eprintln!(
                "[FILL] read_ahead_position: ({}, {})",
                pos.segment_id, pos.offset
            );
            (pos.segment_id, pos.offset)
        };

        // 从 LogReader 读取原始数据（不更新 LogReader.position）
        eprintln!(
            "[FILL] calling read_raw_at({}, {}, {})",
            segment_id, offset, self.read_ahead_size
        );
        let read_result = {
            let reader = self.reader.read().await;
            reader
                .read_raw_at(segment_id, offset, self.read_ahead_size)
                .await?
        };

        eprintln!(
            "[FILL] read_raw_at completed: len={}",
            read_result.data.len()
        );

        if read_result.data.is_empty() {
            eprintln!("[FILL] empty data, setting exhausted");
            buffer.exhausted = true;
            return Err(Error::Eof);
        }

        eprintln!(
            "[FILL] filling buffer with {} bytes",
            read_result.data.len()
        );
        let preserved_len = buffer.fill(read_result.data);

        // 方案 C+：更新 read_ahead_position
        // 简化逻辑：直接使用 end_offset 作为下一次读取的起始位置
        // 保留的数据已经在缓冲区中，不需要重新读取
        {
            let mut pos = self.read_ahead_position.write().await;
            pos.segment_id = read_result.end_segment_id;
            pos.offset = read_result.end_offset;

            eprintln!(
                "[FILL] updated read_ahead_position to ({}, {}), preserved {} bytes",
                pos.segment_id, pos.offset, preserved_len
            );
        }

        eprintln!("[FILL] complete");
        Ok(())
    }

    /// 获取底层读取器
    pub async fn reader(&self) -> Arc<RwLock<LogReader>> {
        self.reader.clone()
    }

    /// 获取预读大小
    pub fn read_ahead_size(&self) -> usize {
        self.read_ahead_size
    }

    /// 读取下一条记录
    ///
    /// 优先从预读缓冲区读取，缓冲区耗尽时自动填充
    /// 格式：[4 字节 magic][4 字节长度][4 字节 CRC32][数据...]
    pub async fn read_next(&self) -> Result<Vec<u8>> {
        // 尝试从预读缓冲区读取
        if let Some(data) = self.read_from_buffer().await {
            return Ok(data);
        }

        // 缓冲区为空，填充缓冲区
        match self.fill_buffer().await {
            Ok(()) => {
                // 再次尝试从缓冲区读取
                if let Some(data) = self.read_from_buffer().await {
                    return Ok(data);
                }
                // 缓冲区填充后仍无数据或无法解析，说明到达末尾
                Err(Error::Eof)
            }
            Err(Error::Eof) => Err(Error::Eof),
            Err(e) => Err(e),
        }
    }

    /// 批量顺序读取
    ///
    /// 利用预读缓冲区优化批量读取性能
    pub async fn read_batch(&self, max_count: usize) -> Result<Vec<Vec<u8>>> {
        let mut records = Vec::with_capacity(max_count);

        // 预先填充缓冲区
        let _ = self.fill_buffer().await;

        for _ in 0..max_count {
            match self.read_next().await {
                Ok(data) => records.push(data),
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// 跳转到指定位置（清除预读缓冲）
    ///
    /// 方案 C+：同时重置 consume_position 和 read_ahead_position。
    pub async fn seek(&self, segment_id: u64, offset: u64) {
        let mut buffer = self.read_ahead_buffer.write().await;
        buffer.clear();

        // 方案 C+：同时更新消费位置和预读位置
        {
            let mut pos = self.consume_position.write().await;
            pos.segment_id = segment_id;
            pos.offset = offset;
        }
        {
            let mut pos = self.read_ahead_position.write().await;
            pos.segment_id = segment_id;
            pos.offset = offset;
        }

        // 同步到 LogReader（保持底层 IO 状态一致）
        let reader = self.reader.read().await;
        reader.seek(segment_id, offset).await;
    }

    /// 跳转到开头
    ///
    /// 方案 C+：同时重置 consume_position 和 read_ahead_position。
    pub async fn seek_to_start(&self) {
        {
            let mut buffer = self.read_ahead_buffer.write().await;
            buffer.clear();
        }

        // 方案 C+：同时重置消费位置和预读位置到开头
        {
            let mut pos = self.consume_position.write().await;
            pos.segment_id = 1;
            pos.offset = crate::storage::format::SEGMENT_HEADER_SIZE;
        }
        {
            let mut pos = self.read_ahead_position.write().await;
            pos.segment_id = 1;
            pos.offset = crate::storage::format::SEGMENT_HEADER_SIZE;
        }

        // 同步到 LogReader
        {
            let reader = self.reader.read().await;
            reader.seek_to_start().await;
        }

        // 预填充缓冲区
        let _ = self.fill_buffer().await;
    }

    /// 获取当前位置（返回消费位置，即已解析的记录位置）
    ///
    /// 方案 C+：返回 consume_position，不是 read_ahead_position。
    /// 这确保 position() 始终反映用户实际消费的位置，而不是预读 IO 的位置。
    pub async fn position(&self) -> ReadPosition {
        let pos = self.consume_position.read().await;
        ReadPosition {
            segment_id: pos.segment_id,
            offset: pos.offset,
        }
    }

    /// 获取段信息
    pub async fn segments(&self) -> Vec<SegmentMeta> {
        let reader = self.reader.read().await;
        reader.segments().await
    }

    /// 获取段数量
    pub async fn segment_count(&self) -> usize {
        let reader = self.reader.read().await;
        reader.segment_count().await
    }

    /// 关闭读取协调器
    pub async fn close(&self) -> Result<()> {
        let reader = self.reader.read().await;
        reader.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{LogReaderConfig, SegmentConfig};
    use crate::wal::{RotationConfig, SegmentCoordinator};
    use tempfile::tempdir;

    /// 辅助函数：创建测试用的 WriteCoordinator
    async fn create_test_coordinator(
        dir: &std::path::Path,
        sync_mode: SyncMode,
    ) -> Arc<WriteCoordinator> {
        let rotation_config = RotationConfig::new();
        let segment_config = SegmentConfig::new(dir);
        let segment_coordinator = Arc::new(
            SegmentCoordinator::new(rotation_config, segment_config)
                .await
                .unwrap(),
        );
        Arc::new(WriteCoordinator::new(segment_coordinator, sync_mode))
    }

    #[tokio::test]
    async fn test_write_coordinator_basic() {
        let temp_dir = tempdir().unwrap();
        let coordinator = create_test_coordinator(temp_dir.path(), SyncMode::FsyncOnWrite).await;

        let pos = coordinator.write(b"test data").await.unwrap();
        assert_eq!(pos.segment_id, 1);
        assert_eq!(pos.length, 9);

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_sync_mode() {
        let temp_dir = tempdir().unwrap();
        let coordinator =
            create_test_coordinator(temp_dir.path(), SyncMode::Batch { batch_size: 3 }).await;

        // 前两次写入不应该触发同步
        coordinator.write(b"data1").await.unwrap();
        coordinator.write(b"data2").await.unwrap();

        // 第三次写入应该触发同步
        coordinator.write(b"data3").await.unwrap();

        // 检查统计信息
        let stats = coordinator.sync_stats().await;
        assert_eq!(stats.sync_count, 1);

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_fsync_on_write_mode() {
        let temp_dir = tempdir().unwrap();
        let coordinator = create_test_coordinator(temp_dir.path(), SyncMode::FsyncOnWrite).await;

        // 每次写入都应该触发同步
        coordinator.write(b"data1").await.unwrap();
        coordinator.write(b"data2").await.unwrap();

        let stats = coordinator.sync_stats().await;
        assert_eq!(stats.sync_count, 2);

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sync_returns_report() {
        let temp_dir = tempdir().unwrap();
        // 使用 SyncMode::None，避免自动同步，确保手动 sync 有实际数据需要同步
        let coordinator = create_test_coordinator(temp_dir.path(), SyncMode::None).await;

        // 写入数据（不会自动同步）
        coordinator.write(b"test data").await.unwrap();

        // 手动同步并获取报告（现在会有实际数据需要同步）
        let report = coordinator.sync().await.unwrap();
        assert!(report.success);
        assert!(report.error.is_none());

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_periodic_sync_mode() {
        let temp_dir = tempdir().unwrap();
        let coordinator =
            create_test_coordinator(temp_dir.path(), SyncMode::Periodic { interval_ms: 100 }).await;

        // 写入数据不应该触发同步
        coordinator.write(b"data1").await.unwrap();
        coordinator.write(b"data2").await.unwrap();

        let stats = coordinator.sync_stats().await;
        assert_eq!(stats.sync_count, 0);

        // 手动调用 check_periodic_sync，刚创建时不应该同步
        coordinator.check_periodic_sync().await.unwrap();

        let stats = coordinator.sync_stats().await;
        assert_eq!(stats.sync_count, 0);

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_read_coordinator_basic() {
        let temp_dir = tempdir().unwrap();

        let write_coord = create_test_coordinator(temp_dir.path(), SyncMode::FsyncOnWrite).await;
        write_coord.write(b"hello").await.unwrap();
        write_coord.close().await.unwrap();

        let reader_config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = Arc::new(RwLock::new(LogReader::new(reader_config).await.unwrap()));
        let coordinator = ReadCoordinator::new(reader).await;

        coordinator.seek_to_start().await;
        let result = coordinator.read_next().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"hello");

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_read_write_round_trip() {
        let temp_dir = tempdir().unwrap();

        let write_coord = create_test_coordinator(temp_dir.path(), SyncMode::FsyncOnWrite).await;

        write_coord.write(b"record1").await.unwrap();
        write_coord.write(b"record2").await.unwrap();
        write_coord.write(b"record3").await.unwrap();
        write_coord.close().await.unwrap();

        let reader_config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = Arc::new(RwLock::new(LogReader::new(reader_config).await.unwrap()));
        let read_coord = ReadCoordinator::new(reader).await;

        read_coord.seek_to_start().await;

        // 读取所有记录直到 EOF
        let mut count = 0;
        while let Ok(data) = read_coord.read_next().await {
            count += 1;
            assert!(!data.is_empty());
        }

        // 至少能读到3条记录
        assert!(count >= 3);

        read_coord.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_write_with_sync() {
        let temp_dir = tempdir().unwrap();

        let write_coord =
            create_test_coordinator(temp_dir.path(), SyncMode::Batch { batch_size: 5 }).await;

        // 批量写入 3 条数据，不应该触发同步
        let data_list: Vec<&[u8]> = vec![b"a", b"bb", b"ccc"];
        let positions = write_coord.write_batch(&data_list).await.unwrap();
        assert_eq!(positions.len(), 3);

        let stats = write_coord.sync_stats().await;
        assert_eq!(stats.sync_count, 0);

        // 批量写入 3 条数据，总共 6 条，超过 batch_size=5
        let data_list2: Vec<&[u8]> = vec![b"dddd", b"eeeee", b"ffffff"];
        let positions2 = write_coord.write_batch(&data_list2).await.unwrap();
        assert_eq!(positions2.len(), 3);

        let stats = write_coord.sync_stats().await;
        assert_eq!(stats.sync_count, 1);

        write_coord.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_manual_sync() {
        let temp_dir = tempdir().unwrap();

        let write_coord = create_test_coordinator(temp_dir.path(), SyncMode::None).await;

        // None 模式下写入不会自动同步
        write_coord.write(b"data").await.unwrap();

        let stats = write_coord.sync_stats().await;
        assert_eq!(stats.sync_count, 0);

        // 手动同步
        write_coord.sync().await.unwrap();

        let stats = write_coord.sync_stats().await;
        assert_eq!(stats.sync_count, 1);

        write_coord.close().await.unwrap();
    }
}
