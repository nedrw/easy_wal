//! Writer Handle - 独立 Writer 的句柄
//!
//! 提供写入接口，可以由单个线程或任务持有。

use crate::prelude::*;
use crate::storage::WritePosition;
use crate::wal::commit_coordinator::{CommitCoordinator, WriterId, WriterMeta, WriterStats};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Writer 句柄
///
/// 代表一个独立的 writer，提供写入接口。
/// 可以被单个线程或任务持有。
pub struct WriterHandle {
    /// Writer ID
    writer_id: WriterId,
    /// CommitCoordinator 引用
    coordinator: Arc<CommitCoordinator>,
    /// Writer 元数据
    meta: Arc<WriterMeta>,
    /// 本地统计（用于快速更新）
    local_stats: Mutex<WriterStats>,
    /// 关闭标志
    closed: AtomicBool,
}

impl WriterHandle {
    /// 创建新的 WriterHandle（内部方法）
    pub(crate) fn new(
        writer_id: WriterId,
        coordinator: Arc<CommitCoordinator>,
        meta: Arc<WriterMeta>,
    ) -> Self {
        Self {
            writer_id,
            coordinator,
            meta,
            local_stats: Mutex::new(WriterStats::default()),
            closed: AtomicBool::new(false),
        }
    }

    /// 获取 Writer ID
    pub fn id(&self) -> WriterId {
        self.writer_id
    }

    /// 获取 Writer 名称
    pub fn name(&self) -> Option<&str> {
        self.meta.name.as_deref()
    }

    /// 写入单条数据
    pub async fn write(&self, data: &[u8]) -> Result<WritePosition> {
        self.write_batch(&[data]).await.map(|mut v| v.remove(0))
    }

    /// 批量写入
    pub async fn write_batch(&self, data_list: &[&[u8]]) -> Result<Vec<WritePosition>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Generic("Writer is closed".to_string()));
        }

        if data_list.is_empty() {
            return Ok(Vec::new());
        }

        // 创建 WriteBatch
        let records: Vec<Vec<u8>> = data_list.iter().map(|d| d.to_vec()).collect();
        let (batch, result_rx) = crate::wal::WriteBatch::new(self.writer_id, records);

        // 提交到 CommitCoordinator
        self.coordinator.add_batch(batch).await;

        // 等待结果
        let positions = result_rx
            .await
            .map_err(|_| Error::Generic("Commit channel closed".to_string()))??;

        // 更新统计
        self.update_stats(data_list).await;

        Ok(positions)
    }

    /// 更新统计信息
    async fn update_stats(&self, data_list: &[&[u8]]) {
        // 更新本地统计
        {
            let mut local_stats = self.local_stats.lock().unwrap();
            local_stats.write_count += 1;
            local_stats.write_records += data_list.len() as u64;
            local_stats.write_bytes += data_list.iter().map(|d| d.len() as u64).sum::<u64>();
        }

        // 定期更新全局统计（避免频繁锁竞争）
        let should_update = {
            let local_stats = self.local_stats.lock().unwrap();
            local_stats.write_count % 10 == 0
        };

        if should_update {
            let local_stats = self.local_stats.lock().unwrap().clone();

            // 更新 meta 中的统计
            if let Ok(mut stats) = self.meta.stats.lock() {
                stats.write_count += local_stats.write_count;
                stats.write_records += local_stats.write_records;
                stats.write_bytes += local_stats.write_bytes;
            }

            // 重置本地统计
            *self.local_stats.lock().unwrap() = WriterStats::default();
        }
    }

    /// 关闭 writer
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(()); // 已经关闭
        }

        // 从注册表注销
        self.coordinator.unregister_writer(self.writer_id).await?;

        // 刷新剩余统计
        let local_stats = self.local_stats.lock().unwrap().clone();
        if let Ok(mut stats) = self.meta.stats.lock() {
            stats.write_count += local_stats.write_count;
            stats.write_records += local_stats.write_records;
            stats.write_bytes += local_stats.write_bytes;
        }

        tracing::debug!("Writer {} closed", self.writer_id);
        Ok(())
    }
}

impl Drop for WriterHandle {
    fn drop(&mut self) {
        // 标记为关闭
        self.closed.store(true, Ordering::Release);
    }
}
