//! WAL 管理器 - API 层
//!
//! # 教学价值
//! - 学习 API 设计
//! - 学习 Builder 模式
//! - 学习组件集成

use super::{
    Checkpoint, CheckpointPosition, CommitConfig, CommitCoordinator, CommitStats, ReadCoordinator,
    RecoveryManager, RecoveryMode, RecoveryResult, RotationConfig, SegmentCoordinator, WriteMode,
    WriterHandle,
};
use crate::prelude::*;
use crate::storage::{LogReader, LogReaderConfig, SegmentConfig, WritePosition};
use std::path::Path;
use std::sync::Arc;

/// WAL 配置
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// 目录路径
    pub dir: std::path::PathBuf,
    /// 最大段大小
    pub max_segment_size: u64,
    /// 批量大小
    pub batch_size: usize,
    /// 预读缓冲区大小
    pub read_ahead_size: usize,
    /// Commit 配置（可选）
    pub commit_config: Option<CommitConfig>,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: std::path::PathBuf::from("wal_data"),
            max_segment_size: 64 * 1024 * 1024, // 64MB
            batch_size: 100,
            read_ahead_size: 64 * 1024, // 64KB
            commit_config: None,
        }
    }
}

impl WalConfig {
    pub fn with_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.dir = dir.as_ref().to_path_buf();
        self
    }

    pub fn with_max_segment_size(mut self, size: u64) -> Self {
        self.max_segment_size = size;
        self
    }

    /// 设置 Commit 配置
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    pub fn with_read_ahead_size(mut self, size: usize) -> Self {
        self.read_ahead_size = size;
        self
    }

    /// 设置 Commit 配置
    pub fn with_commit_config(mut self, config: CommitConfig) -> Self {
        self.commit_config = Some(config);
        self
    }
}

/// WAL 记录
#[derive(Debug, Clone)]
pub struct Record {
    /// 数据
    pub data: Vec<u8>,
    /// 写入位置
    pub position: WritePosition,
}

/// WAL 管理器（简化版）
///
/// 统一使用 CommitCoordinator，无需区分单写/多写模式。
pub struct WalManager {
    /// CommitCoordinator（唯一协调器）
    commit_coordinator: Arc<CommitCoordinator>,
    /// ReadCoordinator
    read_coordinator: Arc<ReadCoordinator>,
    /// RecoveryManager
    recovery_manager: RecoveryManager,
    /// 配置
    config: WalConfig,
}

impl WalManager {
    /// 创建 WAL 管理器
    pub async fn new(config: WalConfig) -> Result<Self> {
        // 创建目录
        tokio::fs::create_dir_all(&config.dir).await?;

        // 创建段协调器（新增）
        let rotation_config = RotationConfig::new().with_max_size(config.max_segment_size);
        let segment_config = SegmentConfig::new(&config.dir);
        let segment_coordinator =
            Arc::new(SegmentCoordinator::new(rotation_config, segment_config).await?);

        // 创建 CommitCoordinator（统一协调器）
        let commit_config = config.commit_config.clone().unwrap_or_default();
        let commit_coordinator =
            Arc::new(CommitCoordinator::new(commit_config, segment_coordinator).await?);

        // 启动 commit loop
        commit_coordinator.start();

        // 创建读取器（不变）
        let reader_config = LogReaderConfig::default()
            .with_dir(&config.dir)
            .with_batch_size(config.batch_size);
        let reader = Arc::new(tokio::sync::RwLock::new(
            LogReader::new(reader_config).await?,
        ));

        // 创建读取协调器（不变）
        let read_coordinator =
            Arc::new(ReadCoordinator::new(reader).with_read_ahead(config.read_ahead_size));

        // 创建恢复管理器（不变）
        let recovery_manager = RecoveryManager::new(&config.dir);

        Ok(Self {
            commit_coordinator,
            read_coordinator,
            recovery_manager,
            config,
        })
    }

    /// 恢复 WAL
    ///
    /// 在 WAL 启动时调用，恢复崩溃前的状态。
    pub async fn recover(&self, mode: RecoveryMode) -> Result<RecoveryResult> {
        // 使用指定模式创建临时恢复管理器
        let recovery_manager = RecoveryManager::new(&self.config.dir).with_mode(mode);
        let result = recovery_manager.recover().await?;

        // 将读取器跳转到恢复位置
        if let Some(pos) = recovery_manager.get_recovery_position().await? {
            self.read_coordinator.seek(pos.segment_id, pos.offset).await;
        }

        Ok(result)
    }

    /// 创建检查点
    pub async fn checkpoint(&self) -> Result<Checkpoint> {
        let pos = self.read_coordinator.position().await;

        let checkpoint = self
            .recovery_manager
            .create_checkpoint(pos.segment_id, pos.offset, pos.offset)
            .await?;

        Ok(checkpoint)
    }

    /// 获取恢复位置
    pub async fn get_recovery_position(&self) -> Result<Option<CheckpointPosition>> {
        self.recovery_manager.get_recovery_position().await
    }

    /// 注册 writer，返回 WriterHandle
    ///
    /// 单个 writer 时自动优化为直接写入，
    /// 多个 writer 时自动启用 Group Commit。
    pub async fn register_writer(&self, name: Option<String>) -> Result<WriterHandle> {
        self.commit_coordinator.register_writer(name).await
    }

    /// 写入单条数据（便捷方法）
    ///
    /// 内部创建临时 writer，适用于简单场景。
    /// 高性能场景建议使用 `register_writer()` 获取持久化 writer。
    pub async fn write(&self, data: &[u8]) -> Result<WritePosition> {
        let writer = self.register_writer(None).await?;
        let pos = writer.write(data).await?;
        writer.close().await?;
        Ok(pos)
    }

    /// 批量写入（便捷方法）
    ///
    /// 内部创建临时 writer，适用于简单场景。
    /// 高性能场景建议使用 `register_writer()` 获取持久化 writer。
    pub async fn write_batch(&self, data_list: &[&[u8]]) -> Result<Vec<WritePosition>> {
        let writer = self.register_writer(None).await?;
        let positions = writer.write_batch(data_list).await?;
        writer.close().await?;
        Ok(positions)
    }

    /// 读取下一条记录
    pub async fn read(&self) -> Result<Record> {
        // read_next 已经读取了长度前缀，返回的是实际数据
        let data = self.read_coordinator.read_next().await?;
        let length = data.len() as u64;

        // 获取读取后的位置
        let pos = self.read_coordinator.position().await;

        Ok(Record {
            data,
            position: WritePosition {
                segment_id: pos.segment_id,
                offset: pos.offset,
                length,
            },
        })
    }

    /// 批量读取
    pub async fn read_batch(&self, max_count: usize) -> Result<Vec<Record>> {
        let mut records = Vec::with_capacity(max_count);

        for _ in 0..max_count {
            match self.read().await {
                Ok(record) => records.push(record),
                Err(Error::Eof) => break,
                Err(e) => return Err(e),
            }
        }

        Ok(records)
    }

    /// 跳到指定位置
    pub async fn seek(&self, segment_id: u64, offset: u64) {
        self.read_coordinator.seek(segment_id, offset).await;
    }

    /// 跳到开头
    pub async fn seek_to_start(&self) {
        self.read_coordinator.seek_to_start().await;
    }

    /// 获取当前位置
    pub async fn position(&self) -> super::ReadPosition {
        self.read_coordinator.position().await
    }

    /// 获取所有段信息
    pub async fn segments(&self) -> Vec<super::SegmentMeta> {
        self.read_coordinator.segments().await
    }

    /// 获取当前写入模式
    pub fn write_mode(&self) -> WriteMode {
        self.commit_coordinator.mode()
    }

    /// 获取活跃 writer 数量
    pub async fn writer_count(&self) -> u64 {
        self.commit_coordinator.writer_count().await
    }

    /// 获取统计信息
    pub async fn stats(&self) -> CommitStats {
        self.commit_coordinator.stats().await
    }

    /// 强制刷新
    pub async fn flush(&self) -> Result<()> {
        self.commit_coordinator.flush().await
    }

    /// 关闭 WAL
    pub async fn close(&self) -> Result<()> {
        // 关闭 CommitCoordinator
        self.commit_coordinator.shutdown().await;

        // 关闭 ReadCoordinator
        self.read_coordinator.close().await
    }

    // ============================================================
    // 监控和配置热更新
    // ============================================================

    /// 获取当前同步模式（向后兼容，实际由 CommitConfig 控制）
    /// 获取同步统计信息
    /// 返回 CommitStats（包含批次统计信息）
    pub async fn sync_stats(&self) -> CommitStats {
        self.commit_coordinator.stats().await
    }
}

/// WAL 构建器
pub struct WalBuilder {
    config: WalConfig,
}

impl WalBuilder {
    pub fn new() -> Self {
        Self {
            config: WalConfig::default(),
        }
    }

    pub fn with_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.config.dir = dir.as_ref().to_path_buf();
        self
    }

    pub fn with_max_segment_size(mut self, size: u64) -> Self {
        self.config.max_segment_size = size;
        self
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    pub fn with_read_ahead_size(mut self, size: usize) -> Self {
        self.config.read_ahead_size = size;
        self
    }

    pub async fn build(&self) -> Result<WalManager> {
        WalManager::new(self.config.clone()).await
    }
}

impl Default for WalBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_wal_write_read() {
        let temp_dir = tempdir().unwrap();

        let wal = WalBuilder::new()
            .with_dir(temp_dir.path())
            .with_sync_mode(SyncMode::FsyncOnWrite)
            .build()
            .await
            .unwrap();

        // 写入数据
        let pos = wal.write(b"hello world").await.unwrap();
        assert_eq!(pos.segment_id, 1);

        // 跳到开头读取
        wal.seek_to_start().await;

        // 读取数据
        let record = wal.read().await.unwrap();
        assert_eq!(record.data, b"hello world");

        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_batch_write_read() {
        let temp_dir = tempdir().unwrap();

        let wal = WalBuilder::new()
            .with_dir(temp_dir.path())
            .with_sync_mode(SyncMode::FsyncOnWrite)
            .build()
            .await
            .unwrap();

        // 批量写入
        let data_list: Vec<&[u8]> = vec![b"a", b"bb", b"ccc", b"dddd"];
        let positions = wal.write_batch(&data_list).await.unwrap();
        assert_eq!(positions.len(), 4);

        // 跳到开头批量读取
        wal.seek_to_start().await;
        let records = wal.read_batch(10).await.unwrap();

        assert!(records.len() >= 4);
        assert_eq!(records[0].data, b"a");
        assert_eq!(records[1].data, b"bb");

        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_seek() {
        let temp_dir = tempdir().unwrap();

        let wal = WalBuilder::new()
            .with_dir(temp_dir.path())
            .with_sync_mode(SyncMode::FsyncOnWrite)
            .build()
            .await
            .unwrap();

        wal.write(b"first").await.unwrap();
        wal.write(b"second").await.unwrap();

        // 跳到位置 0 读取第一条 (实际上应该跳过 16 字节段头)
        // 新格式: [16字节段头][记录1][记录2]...
        wal.seek(1, 16).await;
        let record = wal.read().await.unwrap();
        assert_eq!(record.data, b"first");

        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_segments() {
        let temp_dir = tempdir().unwrap();

        let wal = WalBuilder::new()
            .with_dir(temp_dir.path())
            .with_max_segment_size(10)
            .with_sync_mode(SyncMode::FsyncOnWrite)
            .build()
            .await
            .unwrap();

        // 写入数据触发轮转
        wal.write(b"12345678901").await.unwrap();

        let segments = wal.segments().await;
        assert!(segments.len() >= 1);

        wal.close().await.unwrap();
    }

    #[tokio::test]
    async fn test_wal_commit_stats() {
        let temp_dir = tempdir().unwrap();

        // 使用默认配置
        let wal = WalBuilder::new()
            .with_dir(temp_dir.path())
            .build()
            .await
            .unwrap();

        // 写入一些数据
        wal.write(b"test data").await.unwrap();

        // 在单写模式下，direct_write 会同步更新统计数据
        // 等待一小段时间确保 commit loop 处理完成（如果有多写模式）
        tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

        // 检查统计
        let stats = wal.sync_stats().await;
        assert!(
            stats.total_batches > 0,
            "At least one batch should have been committed"
        );
        assert!(
            stats.total_records > 0,
            "At least one record should have been written"
        );

        wal.close().await.unwrap();
    }
}
