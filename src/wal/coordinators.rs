//! WAL 协调器 - 读写协调组件
//!
//! # 教学价值
//! - 学习协调器模式
//! - 学习读写分离
//! - 学习缓冲优化

use crate::prelude::*;
use crate::storage::{LogReader, LogWriter, ReadPosition, SegmentMeta, WritePosition};
use crate::wal::{SyncContext, SyncMode, SyncStats};
use std::sync::Arc;
use tokio::sync::RwLock;

/// 最大单条记录大小 (64MB)
const MAX_RECORD_SIZE: u64 = 64 * 1024 * 1024;

/// Sync 完成回调类型
///
/// 回调会在每次 sync 操作完成后被调用，无论成功或失败。
/// 回调参数为 (duration_ms, error)，error 为 None 表示成功。
pub type SyncCallback = Box<dyn Fn(u64, Option<String>) + Send + Sync>;

// ============================================================
// 写入协调器
// ============================================================

/// 写入协调器
///
/// 协调写入操作，提供：
/// - 同步策略执行
/// - 批量写入优化
/// - 与 RecoveryManager 协作
pub struct WriteCoordinator {
    writer: Arc<LogWriter>,
    sync_context: RwLock<SyncContext>,
    /// 同步统计信息
    sync_stats: RwLock<SyncStats>,
    /// Sync 完成回调
    sync_callback: RwLock<Option<SyncCallback>>,
}

impl WriteCoordinator {
    /// 创建写入协调器
    pub fn new(writer: Arc<LogWriter>, sync_mode: SyncMode) -> Self {
        Self {
            writer,
            sync_context: RwLock::new(SyncContext::new(sync_mode)),
            sync_stats: RwLock::new(SyncStats::new()),
            sync_callback: RwLock::new(None),
        }
    }

    /// 设置 Sync 完成回调
    ///
    /// 回调会在每次 sync 操作完成后被调用，包括成功和失败的情况。
    /// 设置新的回调会替换旧的回调。传入 `None` 可以清除回调。
    pub async fn set_sync_callback(&self, callback: Option<SyncCallback>) {
        let mut cb = self.sync_callback.write().await;
        *cb = callback;
    }

    /// 发送 Sync 完成事件（内部使用）
    async fn notify_sync_complete(&self, duration_ms: u64, error: Option<String>) {
        let cb = self.sync_callback.read().await;
        if let Some(callback) = cb.as_ref() {
            callback(duration_ms, error);
        }
    }

    /// 创建批量同步策略的协调器
    pub fn with_batch(writer: Arc<LogWriter>, batch_size: u64) -> Self {
        Self::new(writer, SyncMode::Batch { batch_size })
    }

    /// 创建周期同步策略的协调器
    pub fn with_periodic(writer: Arc<LogWriter>, interval_ms: u64) -> Self {
        Self::new(writer, SyncMode::Periodic { interval_ms })
    }

    /// 获取底层写入器
    pub fn writer(&self) -> Arc<LogWriter> {
        self.writer.clone()
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

        // 执行写入
        let pos = self.writer.write(data).await?;

        // 根据策略决定是否同步
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

        // 执行批量写入
        let positions = self.writer.write_batch(data_list).await?;

        // 根据策略决定是否同步
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
    pub async fn sync(&self) -> Result<()> {
        self.do_sync().await
    }

    /// 执行实际的同步操作
    async fn do_sync(&self) -> Result<()> {
        let start = std::time::Instant::now();
        let result = self.writer.sync().await;
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

                // 通知回调
                self.notify_sync_complete(duration_ms, None).await;
            }
            Err(e) => {
                // 通知回调失败
                self.notify_sync_complete(duration_ms, Some(e.to_string()))
                    .await;
                return Err(e);
            }
        }

        Ok(())
    }

    /// 强制轮转段
    pub async fn rotate(&self) -> Result<(u64, std::path::PathBuf)> {
        self.writer.rotate().await
    }

    /// 获取活跃段 ID
    pub async fn active_segment_id(&self) -> u64 {
        self.writer.active_segment_id().await
    }

    /// 获取所有段信息
    pub async fn segments(&self) -> Vec<SegmentMeta> {
        self.writer.segments().await
    }

    /// 关闭写入协调器
    pub async fn close(&self) -> Result<()> {
        self.sync().await?;
        self.writer.close().await
    }
}

// ============================================================
// 读取协调器
// ============================================================

/// 读取协调器
///
/// 协调读取操作，提供：
/// - 读取缓冲（预读优化）
/// - 并发读取控制
pub struct ReadCoordinator {
    reader: Arc<RwLock<LogReader>>,
    /// 预读缓冲区
    read_ahead_buffer: Arc<RwLock<ReadAheadBuffer>>,
    /// 预读大小
    read_ahead_size: usize,
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
            return None;
        }
        let magic_bytes: [u8; 4] = self.data[self.pos..self.pos + 4].try_into().unwrap();
        let magic = u32::from_be_bytes(magic_bytes);

        // 验证 Magic
        if magic != crate::storage::format::RECORD_MAGIC {
            return None;
        }

        // 读取长度 (4 bytes)
        if self.pos + 8 > self.data.len() {
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

    /// 填充缓冲区
    fn fill(&mut self, data: Vec<u8>) {
        self.data = data;
        self.pos = 0;
        self.exhausted = false;
        self.has_incomplete = false;
    }
}

impl ReadCoordinator {
    /// 创建读取协调器
    pub fn new(reader: Arc<RwLock<LogReader>>) -> Self {
        Self {
            reader,
            read_ahead_buffer: Arc::new(RwLock::new(ReadAheadBuffer::new(64 * 1024))),
            read_ahead_size: 64 * 1024,
        }
    }

    /// 设置预读大小
    pub fn with_read_ahead(mut self, size: usize) -> Self {
        self.read_ahead_size = size;
        self.read_ahead_buffer = Arc::new(RwLock::new(ReadAheadBuffer::new(size)));
        self
    }

    /// 尝试从预读缓冲区读取
    async fn read_from_buffer(&self) -> Option<Vec<u8>> {
        let mut buffer = self.read_ahead_buffer.write().await;
        buffer.read()
    }

    /// 填充预读缓冲区
    async fn fill_buffer(&self) -> Result<()> {
        let mut buffer = self.read_ahead_buffer.write().await;

        // 只有当缓冲区完全为空时才填充
        // 如果缓冲区有残留数据但无法解析，说明是损坏或不完整记录，直接清空
        if buffer.has_data() {
            return Ok(());
        }

        // 从 LogReader 读取原始数据填充缓冲区
        let raw_data = {
            let reader = self.reader.read().await;
            reader.read_raw(self.read_ahead_size).await?
        };

        if raw_data.is_empty() {
            buffer.exhausted = true;
            return Err(Error::Eof);
        }

        buffer.fill(raw_data);
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
    pub async fn seek(&self, segment_id: u64, offset: u64) {
        let mut buffer = self.read_ahead_buffer.write().await;
        buffer.clear();

        let reader = self.reader.read().await;
        reader.seek(segment_id, offset).await;
    }

    /// 跳转到开头
    pub async fn seek_to_start(&self) {
        {
            let mut buffer = self.read_ahead_buffer.write().await;
            buffer.clear();
        }

        {
            let reader = self.reader.read().await;
            reader.seek_to_start().await;
        }

        // 预填充缓冲区
        let _ = self.fill_buffer().await;
    }

    /// 获取当前位置
    pub async fn position(&self) -> ReadPosition {
        let reader = self.reader.read().await;
        reader.position().await
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
    use crate::storage::{LogReaderConfig, LogWriterConfig};
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_write_coordinator_basic() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default().with_dir(temp_dir.path());

        let writer = Arc::new(LogWriter::new(config).await.unwrap());
        let coordinator = WriteCoordinator::new(writer, SyncMode::FsyncOnWrite);

        let pos = coordinator.write(b"test data").await.unwrap();
        assert_eq!(pos.segment_id, 1);
        assert_eq!(pos.length, 9);

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_batch_sync_mode() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default().with_dir(temp_dir.path());

        let writer = Arc::new(LogWriter::new(config).await.unwrap());
        let coordinator = WriteCoordinator::with_batch(writer, 3);

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
        let config = LogWriterConfig::default().with_dir(temp_dir.path());

        let writer = Arc::new(LogWriter::new(config).await.unwrap());
        let coordinator = WriteCoordinator::new(writer, SyncMode::FsyncOnWrite);

        // 每次写入都应该触发同步
        coordinator.write(b"data1").await.unwrap();
        coordinator.write(b"data2").await.unwrap();

        let stats = coordinator.sync_stats().await;
        assert_eq!(stats.sync_count, 2);

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_sync_callback() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default().with_dir(temp_dir.path());

        let writer = Arc::new(LogWriter::new(config).await.unwrap());
        let coordinator = WriteCoordinator::new(writer, SyncMode::FsyncOnWrite);

        // 用于接收回调数据的通道
        let (tx, mut rx) = tokio::sync::mpsc::channel::<(u64, Option<String>)>(10);

        // 设置回调
        coordinator
            .set_sync_callback(Some(Box::new(move |duration_ms, error| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    tx.send((duration_ms, error)).await.ok();
                });
            })))
            .await;

        // 写入数据触发同步
        coordinator.write(b"test data").await.unwrap();

        // 等待回调被调用
        let (duration_ms, error) = rx.recv().await.unwrap();
        assert!(duration_ms > 0);
        assert!(error.is_none());

        // 清除回调
        coordinator.set_sync_callback(None).await;

        // 再次写入不应该触发回调（因为已清除）
        coordinator.write(b"more data").await.unwrap();

        // 给一点时间确保没有回调
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_periodic_sync_mode() {
        let temp_dir = tempdir().unwrap();
        let config = LogWriterConfig::default().with_dir(temp_dir.path());

        let writer = Arc::new(LogWriter::new(config).await.unwrap());
        // 100ms 间隔
        let coordinator = WriteCoordinator::with_periodic(writer, 100);

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

        let writer_config = LogWriterConfig::default().with_dir(temp_dir.path());
        let writer = Arc::new(LogWriter::new(writer_config).await.unwrap());
        let write_coord = WriteCoordinator::new(writer, SyncMode::FsyncOnWrite);
        write_coord.write(b"hello").await.unwrap();
        write_coord.close().await.unwrap();

        let reader_config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = Arc::new(RwLock::new(LogReader::new(reader_config).await.unwrap()));
        let coordinator = ReadCoordinator::new(reader);

        coordinator.seek_to_start().await;
        let result = coordinator.read_next().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), b"hello");

        coordinator.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_read_write_round_trip() {
        let temp_dir = tempdir().unwrap();

        let writer_config = LogWriterConfig::default().with_dir(temp_dir.path());
        let writer = Arc::new(LogWriter::new(writer_config).await.unwrap());
        let write_coord = WriteCoordinator::new(writer, SyncMode::FsyncOnWrite);

        write_coord.write(b"record1").await.unwrap();
        write_coord.write(b"record2").await.unwrap();
        write_coord.write(b"record3").await.unwrap();
        write_coord.close().await.unwrap();

        let reader_config = LogReaderConfig::default().with_dir(temp_dir.path());
        let reader = Arc::new(RwLock::new(LogReader::new(reader_config).await.unwrap()));
        let read_coord = ReadCoordinator::new(reader);

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

        let writer_config = LogWriterConfig::default().with_dir(temp_dir.path());
        let writer = Arc::new(LogWriter::new(writer_config).await.unwrap());
        let write_coord = WriteCoordinator::with_batch(writer, 5);

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

        let writer_config = LogWriterConfig::default().with_dir(temp_dir.path());
        let writer = Arc::new(LogWriter::new(writer_config).await.unwrap());
        let write_coord = WriteCoordinator::new(writer, SyncMode::None);

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
