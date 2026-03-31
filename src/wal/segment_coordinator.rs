//! 段协调器 - 负责段轮转策略决策和段生命周期管理
//!
//! # 设计目标
//! - 集中管理段轮转策略
//! - 为 LogSegment 提供统一的段管理接口（Kafka 模式）
//! - 支持 Multi-Writer 场景
//! - 读写在同一段内共享状态

use crate::prelude::*;
use crate::storage::{FileStorage, LogSegment, SegmentConfig, SegmentMeta, SegmentStats};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

/// 轮转策略配置
#[derive(Debug, Clone)]
pub struct RotationConfig {
    /// 基于大小的轮转阈值（字节）
    pub max_segment_size: u64,
    /// 基于时间的轮转阈值（毫秒，可选）
    pub max_segment_age_ms: Option<u64>,
    /// 基于记录数的轮转阈值（可选）
    pub max_records_per_segment: Option<u64>,
}

impl RotationConfig {
    /// 创建默认配置
    pub fn new() -> Self {
        Self {
            max_segment_size: 1024 * 1024 * 1024, // 1GB
            max_segment_age_ms: None,
            max_records_per_segment: None,
        }
    }

    /// 设置最大段大小
    pub fn with_max_size(mut self, size: u64) -> Self {
        self.max_segment_size = size;
        self
    }

    /// 设置最大段年龄（毫秒）
    pub fn with_max_age(mut self, age_ms: u64) -> Self {
        self.max_segment_age_ms = Some(age_ms);
        self
    }

    /// 设置最大记录数
    pub fn with_max_records(mut self, records: u64) -> Self {
        self.max_records_per_segment = Some(records);
        self
    }
}

impl Default for RotationConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// 段统计信息（扩展）
#[derive(Debug, Clone)]
pub struct ExtendedSegmentStats {
    /// 基础段统计
    pub base: SegmentStats,
    /// 当前段已写入记录数
    pub records_in_current_segment: u64,
    /// 当前段创建时间
    pub current_segment_created_at: Option<Instant>,
}

/// 段协调器
///
/// 负责段轮转策略决策和段生命周期管理。
/// 为写入器提供统一的段管理接口。
pub struct SegmentCoordinator {
    /// 段管理器（底层操作）
    segment_manager: RwLock<crate::storage::SegmentManager>,

    /// 活跃段的 LogSegment（读写共享）
    active_segment: RwLock<Option<Arc<LogSegment>>>,

    /// 轮转策略配置
    rotation_config: RotationConfig,

    /// 段配置
    segment_config: SegmentConfig,

    /// 统计信息
    stats: RwLock<ExtendedSegmentStats>,
}

impl SegmentCoordinator {
    /// 创建段协调器
    pub async fn new(
        rotation_config: RotationConfig,
        segment_config: SegmentConfig,
    ) -> Result<Self> {
        let segment_manager = crate::storage::SegmentManager::new(segment_config.clone())
            .map_err(|e| Error::Generic(format!("Failed to create segment manager: {}", e)))?;

        let base_stats = segment_manager.stats();
        let stats = ExtendedSegmentStats {
            base: base_stats,
            records_in_current_segment: 0,
            current_segment_created_at: None,
        };

        Ok(Self {
            segment_manager: RwLock::new(segment_manager),
            active_segment: RwLock::new(None),
            rotation_config,
            segment_config,
            stats: RwLock::new(stats),
        })
    }

    /// 获取当前活跃段的 LogSegment
    ///
    /// 如果没有活跃段，会自动创建。
    /// 用于写入和读取当前活跃段。
    pub async fn get_active_segment(&self) -> Result<Arc<LogSegment>> {
        // 1. 检查是否有活跃段
        {
            let segment = self.active_segment.read().await;
            if let Some(ref s) = *segment {
                return Ok(s.clone());
            }
        }

        // 2. 需要创建活跃段
        let mut manager = self.segment_manager.write().await;

        // 如果没有活跃段，创建第一个
        if manager.active_id() == 0 {
            manager
                .create_segment()
                .map_err(|e| Error::Generic(format!("Failed to create first segment: {}", e)))?;
        }

        let path = manager.active_path();
        let segment_id = manager.active_id();

        // 创建存储
        let storage = Arc::new(
            FileStorage::new(&path)
                .await
                .map_err(|e| Error::Generic(format!("Failed to create storage: {}", e)))?,
        );

        // 写入段文件头（如果文件为空）
        storage.write_header_if_empty().await?;

        // 创建 LogSegment
        let log_segment = Arc::new(LogSegment::new(storage, segment_id));

        // 保存到活跃段
        let mut active = self.active_segment.write().await;
        *active = Some(log_segment.clone());

        // 更新统计信息：记录段创建时间
        let mut stats = self.stats.write().await;
        stats.current_segment_created_at = Some(Instant::now());

        Ok(log_segment)
    }

    /// 获取指定段的 LogSegment（用于读取历史段）
    ///
    /// # 参数
    /// - `segment_id`: 段 ID
    ///
    /// # 返回
    /// 返回指定段的 LogSegment，如果段不存在则返回 None
    pub async fn get_segment(&self, segment_id: u64) -> Result<Option<Arc<LogSegment>>> {
        // 检查是否是活跃段
        let active_id = {
            let manager = self.segment_manager.read().await;
            manager.active_id()
        };

        if segment_id == active_id {
            // 活跃段：直接返回活跃段的 LogSegment
            return Ok(Some(self.get_active_segment().await?));
        }

        // 历史段：检查段是否存在
        let path = {
            let manager = self.segment_manager.read().await;
            manager.segment_path(segment_id)
        };

        if let Some(path) = path {
            if !path.exists() {
                return Ok(None);
            }

            // 创建存储
            let storage = Arc::new(FileStorage::new(&path).await.map_err(|e| {
                Error::Generic(format!("Failed to open segment {}: {}", segment_id, e))
            })?);

            // 创建 LogSegment（从现有文件）
            let log_segment = Arc::new(LogSegment::from_existing(storage, segment_id).await?);

            Ok(Some(log_segment))
        } else {
            Ok(None)
        }
    }

    /// 更新段大小
    ///
    /// 在写入数据后调用，更新统计信息。
    /// 注意：LogSegment 的 write_position 会自动更新，这里只更新统计信息。
    pub async fn update_size(&self, bytes_written: u64, records_written: u64) {
        let mut manager = self.segment_manager.write().await;
        manager.update_active_size(bytes_written);

        let mut stats = self.stats.write().await;
        stats.records_in_current_segment += records_written;
    }

    /// 检查并执行轮转（如果需要）
    ///
    /// 返回：是否执行了轮转
    pub async fn check_and_rotate(&self) -> Result<bool> {
        let should_rotate = {
            let manager = self.segment_manager.read().await;
            let stats = self.stats.read().await;

            // 决策：是否需要轮转
            let mut should = false;

            // 基于大小的轮转
            if manager.active_size() >= self.rotation_config.max_segment_size {
                should = true;
            }

            // 基于记录数的轮转
            if let Some(max_records) = self.rotation_config.max_records_per_segment {
                if stats.records_in_current_segment >= max_records {
                    should = true;
                }
            }

            // 基于时间的轮转
            if let Some(max_age_ms) = self.rotation_config.max_segment_age_ms {
                if let Some(created_at) = stats.current_segment_created_at {
                    let age_ms = created_at.elapsed().as_millis() as u64;
                    if age_ms >= max_age_ms {
                        should = true;
                    }
                }
            }

            should
        };

        if !should_rotate {
            return Ok(false);
        }

        // 执行轮转
        self.do_rotate().await?;

        Ok(true)
    }

    /// 强制轮转到新段
    pub async fn force_rotate(&self) -> Result<(u64, PathBuf)> {
        self.do_rotate().await
    }

    /// 执行实际的轮转
    async fn do_rotate(&self) -> Result<(u64, PathBuf)> {
        // 1. 创建新段
        let mut manager = self.segment_manager.write().await;
        let (new_id, new_path) = manager
            .create_segment()
            .map_err(|e| Error::Generic(format!("Failed to create segment: {}", e)))?;

        // 2. 清除旧段
        let mut active = self.active_segment.write().await;
        *active = None;

        // 3. 重置统计信息
        let mut stats = self.stats.write().await;
        stats.records_in_current_segment = 0;
        stats.current_segment_created_at = None;
        stats.base = manager.stats();

        Ok((new_id, new_path))
    }

    /// 获取当前活跃段 ID
    pub async fn active_segment_id(&self) -> u64 {
        let manager = self.segment_manager.read().await;
        manager.active_id()
    }

    /// 获取指定段的元数据
    ///
    /// 用于检查段是否存在
    pub async fn get_segment_meta(&self, segment_id: u64) -> Option<SegmentMeta> {
        let manager = self.segment_manager.read().await;
        manager.get_segment(segment_id).cloned()
    }

    /// 获取指定段的路径
    ///
    /// 返回段文件的完整路径
    pub async fn segment_path(&self, segment_id: u64) -> Option<PathBuf> {
        let manager = self.segment_manager.read().await;
        manager.segment_path(segment_id)
    }

    /// 获取当前活跃段大小
    pub async fn active_segment_size(&self) -> u64 {
        let manager = self.segment_manager.read().await;
        manager.active_size()
    }

    /// 获取段列表
    pub async fn segments(&self) -> Vec<SegmentMeta> {
        let manager = self.segment_manager.read().await;
        manager.segments().to_vec()
    }

    /// 获取段数量
    pub async fn segment_count(&self) -> usize {
        let manager = self.segment_manager.read().await;
        manager.segment_count()
    }

    /// 检查段列表是否为空
    pub async fn segments_is_empty(&self) -> bool {
        let manager = self.segment_manager.read().await;
        manager.segments().is_empty()
    }

    /// 获取段统计信息
    pub async fn stats(&self) -> ExtendedSegmentStats {
        let manager = self.segment_manager.read().await;
        let mut stats = self.stats.write().await;
        stats.base = manager.stats();
        stats.clone()
    }

    /// 获取轮转配置
    pub fn rotation_config(&self) -> &RotationConfig {
        &self.rotation_config
    }

    /// 获取段配置
    pub fn segment_config(&self) -> &SegmentConfig {
        &self.segment_config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_segment_coordinator_creation() {
        let temp_dir = tempdir().unwrap();
        let rotation_config = RotationConfig::new().with_max_size(1000);
        let segment_config = SegmentConfig::new(temp_dir.path());

        let coordinator = SegmentCoordinator::new(rotation_config, segment_config)
            .await
            .unwrap();

        assert_eq!(coordinator.segment_count().await, 0);
    }

    #[tokio::test]
    async fn test_get_active_segment() {
        let temp_dir = tempdir().unwrap();
        let rotation_config = RotationConfig::new().with_max_size(1000);
        let segment_config = SegmentConfig::new(temp_dir.path());

        let coordinator = SegmentCoordinator::new(rotation_config, segment_config)
            .await
            .unwrap();

        // 获取活跃段（会自动创建第一个段）
        let segment = coordinator.get_active_segment().await.unwrap();
        assert_eq!(segment.segment_id(), 1);

        // 再次获取，应该是同一个段
        let segment2 = coordinator.get_active_segment().await.unwrap();
        assert_eq!(segment2.segment_id(), 1);
    }

    #[tokio::test]
    async fn test_write_and_rotate() {
        let temp_dir = tempdir().unwrap();
        let rotation_config = RotationConfig::new().with_max_size(100);
        let segment_config = SegmentConfig::new(temp_dir.path());

        let coordinator = SegmentCoordinator::new(rotation_config, segment_config)
            .await
            .unwrap();

        // 获取活跃段
        let segment = coordinator.get_active_segment().await.unwrap();

        // 写入数据
        let pos = segment.append(b"hello world").await.unwrap();
        assert_eq!(pos.segment_id, 1);

        // 更新大小
        coordinator.update_size(23, 1).await; // 12 字节记录头 + 11 字节数据

        // 检查是否需要轮转（不应轮转）
        let rotated = coordinator.check_and_rotate().await.unwrap();
        assert!(!rotated);

        // 写入更多数据触发轮转
        for _ in 0..10 {
            segment.append(b"test data").await.unwrap();
            coordinator.update_size(21, 1).await; // 12 字节记录头 + 9 字节数据
        }

        // 检查是否需要轮转（应该轮转）
        let rotated = coordinator.check_and_rotate().await.unwrap();
        assert!(rotated);

        // 获取新的段（应该是新段）
        let segment2 = coordinator.get_active_segment().await.unwrap();
        assert_eq!(segment2.segment_id(), 2);
    }

    #[tokio::test]
    async fn test_force_rotate() {
        let temp_dir = tempdir().unwrap();
        let rotation_config = RotationConfig::new().with_max_size(10000);
        let segment_config = SegmentConfig::new(temp_dir.path());

        let coordinator = SegmentCoordinator::new(rotation_config, segment_config)
            .await
            .unwrap();

        // 创建第一个段
        let segment = coordinator.get_active_segment().await.unwrap();
        assert_eq!(segment.segment_id(), 1);

        // 强制轮转
        let (new_id, _path) = coordinator.force_rotate().await.unwrap();
        assert_eq!(new_id, 2);

        // 获取新段
        let segment2 = coordinator.get_active_segment().await.unwrap();
        assert_eq!(segment2.segment_id(), 2);
    }

    #[tokio::test]
    async fn test_rotation_by_records() {
        let temp_dir = tempdir().unwrap();
        let rotation_config = RotationConfig::new()
            .with_max_size(10000)
            .with_max_records(5);
        let segment_config = SegmentConfig::new(temp_dir.path());

        let coordinator = SegmentCoordinator::new(rotation_config, segment_config)
            .await
            .unwrap();

        let segment = coordinator.get_active_segment().await.unwrap();

        // 写入 5 条记录
        for i in 0..5 {
            segment
                .append(format!("record {}", i).as_bytes())
                .await
                .unwrap();
            coordinator.update_size(20, 1).await;
        }

        // 检查是否需要轮转（应该轮转，因为达到最大记录数）
        let rotated = coordinator.check_and_rotate().await.unwrap();
        assert!(rotated);
    }
}
