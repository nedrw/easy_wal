//! 段管理模块 - 实现多文件轮转
//!
//! # 教学价值
//! - 学习文件轮转策略
//! - 学习资源生命周期管理
//! - 学习状态机设计

use crate::prelude::*;
use std::path::{Path, PathBuf};

/// 段配置
#[derive(Debug, Clone)]
pub struct SegmentConfig {
    /// 单个段文件最大字节数
    pub max_segment_size: u64,
    /// 段文件目录
    pub dir: PathBuf,
    /// 段文件名前缀
    pub prefix: String,
    /// 段文件扩展名
    pub extension: String,
}

impl SegmentConfig {
    /// 创建默认配置
    ///
    /// 默认：单文件最大 1GB，前缀 "segment"，扩展名 "wal"
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            max_segment_size: 1024 * 1024 * 1024, // 1GB
            dir: dir.as_ref().to_path_buf(),
            prefix: "segment".to_string(),
            extension: "wal".to_string(),
        }
    }

    /// 自定义配置
    pub fn with_max_size(mut self, size: u64) -> Self {
        self.max_segment_size = size;
        self
    }

    /// 自定义前缀
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// 自定义扩展名
    pub fn with_extension(mut self, ext: impl Into<String>) -> Self {
        self.extension = ext.into();
        self
    }
}

impl Default for SegmentConfig {
    fn default() -> Self {
        Self::new(".")
    }
}

/// 段元数据
#[derive(Debug, Clone)]
pub struct SegmentMeta {
    /// 段 ID
    pub id: u64,
    /// 文件路径
    pub path: PathBuf,
    /// 文件大小（字节）
    pub size: u64,
    /// 是否为当前活跃段
    pub is_active: bool,
}

/// 段管理器
///
/// 负责管理多个段文件的创建、轮转和清理。
///
/// # 设计原则
/// 1. 被动轮转：当活跃段达到大小上限时创建新段
/// 2. 保持引用：保留所有段的元数据，支持读取历史段
/// 3. 延迟删除：标记删除而非立即删除，由外部触发清理
pub struct SegmentManager {
    config: SegmentConfig,
    segments: Vec<SegmentMeta>,
    active_id: u64,
    active_size: u64,
}

impl SegmentManager {
    /// 创建段管理器
    pub fn new(config: SegmentConfig) -> Result<Self> {
        // 确保目录存在
        if !config.dir.exists() {
            std::fs::create_dir_all(&config.dir)
                .map_err(|e| Error::Generic(format!("Failed to create directory: {}", e)))?;
        }

        // 扫描现有段文件
        let segments = Self::scan_segments(&config)?;

        // 确定当前活跃段 ID
        let active_id = segments.iter().map(|s| s.id).max().unwrap_or(0);
        let active_size = segments
            .iter()
            .find(|s| s.id == active_id)
            .map(|s| s.size)
            .unwrap_or(0);

        Ok(Self {
            config,
            segments,
            active_id,
            active_size,
        })
    }

    /// 扫描目录中的现有段文件
    fn scan_segments(config: &SegmentConfig) -> Result<Vec<SegmentMeta>> {
        let mut segments = Vec::new();

        if !config.dir.exists() {
            return Ok(segments);
        }

        for entry in std::fs::read_dir(&config.dir)
            .map_err(|e| Error::Generic(format!("Failed to read directory: {}", e)))?
        {
            let entry =
                entry.map_err(|e| Error::Generic(format!("Failed to read entry: {}", e)))?;
            let path = entry.path();

            // 检查是否为段文件
            if !path.is_file() {
                continue;
            }

            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            if !file_name.starts_with(&config.prefix)
                || !file_name.ends_with(&format!(".{}", config.extension))
            {
                continue;
            }

            // 解析段 ID
            let name_without_ext = file_name
                .strip_prefix(&config.prefix)
                .and_then(|s| s.strip_suffix(&format!(".{}", config.extension)))
                .unwrap_or("");

            let id: u64 = name_without_ext.parse().unwrap_or(0);

            // 获取文件大小
            let size = std::fs::metadata(&path)
                .map_err(|e| Error::Generic(format!("Failed to get metadata: {}", e)))?
                .len();

            segments.push(SegmentMeta {
                id,
                path,
                size,
                is_active: false,
            });
        }

        // 按 ID 排序
        segments.sort_by_key(|s| s.id);

        Ok(segments)
    }

    /// 获取当前活跃段路径
    pub fn active_path(&self) -> PathBuf {
        self.make_path(self.active_id)
    }

    /// 获取当前活跃段 ID
    pub fn active_id(&self) -> u64 {
        self.active_id
    }

    /// 获取当前活跃段大小
    pub fn active_size(&self) -> u64 {
        self.active_size
    }

    /// 检查是否需要轮转
    pub fn should_rotate(&self) -> bool {
        self.active_size >= self.config.max_segment_size
    }

    /// 生成段文件路径
    fn make_path(&self, id: u64) -> PathBuf {
        let file_name = format!("{}{}.{}", self.config.prefix, id, self.config.extension);
        self.config.dir.join(file_name)
    }

    /// 创建新段
    ///
    /// # 返回
    /// 返回新段的 ID 和路径
    pub fn create_segment(&mut self) -> Result<(u64, PathBuf)> {
        let new_id = self.active_id + 1;
        let new_path = self.make_path(new_id);

        // 创建空文件
        std::fs::write(&new_path, b"")
            .map_err(|e| Error::Generic(format!("Failed to create segment file: {}", e)))?;

        // 更新活跃段状态
        if let Some(segment) = self.segments.iter_mut().find(|s| s.id == self.active_id) {
            segment.is_active = false;
        }

        // 添加新段元数据
        self.segments.push(SegmentMeta {
            id: new_id,
            path: new_path.clone(),
            size: 0,
            is_active: true,
        });

        self.active_id = new_id;
        self.active_size = 0;

        Ok((new_id, new_path))
    }

    /// 更新活跃段大小
    ///
    /// 在写入数据后调用，检查是否需要轮转
    pub fn update_active_size(&mut self, written: u64) -> bool {
        self.active_size += written;

        // 如果达到大小上限，需要轮转
        if self.should_rotate() {
            // 标记当前段为非活跃
            if let Some(segment) = self.segments.iter_mut().find(|s| s.id == self.active_id) {
                segment.is_active = false;
                segment.size = self.active_size;
            }
            true
        } else {
            // 更新大小
            if let Some(segment) = self.segments.iter_mut().find(|s| s.id == self.active_id) {
                segment.size = self.active_size;
            }
            false
        }
    }

    /// 轮转到新段
    ///
    /// # 返回
    /// 返回新段的 ID 和路径
    pub fn rotate(&mut self) -> Result<(u64, PathBuf)> {
        self.create_segment()
    }

    /// 获取所有段
    pub fn segments(&self) -> &[SegmentMeta] {
        &self.segments
    }

    /// 获取段数量
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// 获取配置
    pub fn config(&self) -> &SegmentConfig {
        &self.config
    }

    /// 删除指定段
    pub fn remove_segment(&mut self, id: u64) -> Result<Option<PathBuf>> {
        if let Some(pos) = self.segments.iter().position(|s| s.id == id) {
            let removed = self.segments.remove(pos);
            // 删除物理文件
            if removed.path.exists() {
                std::fs::remove_file(&removed.path)
                    .map_err(|e| Error::Generic(format!("Failed to delete segment: {}", e)))?;
            }
            Ok(Some(removed.path))
        } else {
            Ok(None)
        }
    }

    /// 获取指定段元数据
    pub fn get_segment(&self, id: u64) -> Option<&SegmentMeta> {
        self.segments.iter().find(|s| s.id == id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_config_default() {
        let config = SegmentConfig::default();
        assert_eq!(config.max_segment_size, 1024 * 1024 * 1024);
        assert_eq!(config.prefix, "segment");
        assert_eq!(config.extension, "wal");
    }

    #[test]
    fn test_config_builder() {
        let config = SegmentConfig::new("/tmp/wal")
            .with_max_size(1000)
            .with_prefix("test")
            .with_extension("log");

        assert_eq!(config.max_segment_size, 1000);
        assert_eq!(config.prefix, "test");
        assert_eq!(config.extension, "log");
    }

    #[test]
    fn test_create_manager() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig::new(temp_dir.path());

        let manager = SegmentManager::new(config).unwrap();

        assert_eq!(manager.segment_count(), 0);
    }

    #[test]
    fn test_create_segment() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig::new(temp_dir.path());

        let mut manager = SegmentManager::new(config).unwrap();

        let (id, path) = manager.create_segment().unwrap();
        assert_eq!(id, 1);
        assert!(path.exists());
        assert_eq!(manager.active_id(), 1);
    }

    #[test]
    fn test_rotate() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig::new(temp_dir.path()).with_max_size(100);

        let mut manager = SegmentManager::new(config).unwrap();

        // 初始活跃段
        assert_eq!(manager.active_id(), 0);

        // 更新大小触发轮转
        let should_rotate = manager.update_active_size(100);
        assert!(should_rotate);

        // 轮转后创建新段
        let (id, _path) = manager.rotate().unwrap();
        assert_eq!(id, 1);
    }

    #[test]
    fn test_update_size() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig::new(temp_dir.path()).with_max_size(100);

        let mut manager = SegmentManager::new(config).unwrap();

        // 未达到上限
        let should_rotate = manager.update_active_size(50);
        assert!(!should_rotate);
        assert_eq!(manager.active_size(), 50);

        // 达到上限
        let should_rotate = manager.update_active_size(50);
        assert!(should_rotate);
        assert_eq!(manager.active_size(), 100);
    }

    #[test]
    fn test_scan_existing_segments() {
        let temp_dir = tempdir().unwrap();

        // 创建一些段文件
        std::fs::write(temp_dir.path().join("segment0.wal"), "data0").unwrap();
        std::fs::write(temp_dir.path().join("segment1.wal"), "data1").unwrap();
        std::fs::write(temp_dir.path().join("segment2.wal"), "data2").unwrap();

        let config = SegmentConfig::new(temp_dir.path());
        let manager = SegmentManager::new(config).unwrap();

        assert_eq!(manager.segment_count(), 3);
    }

    #[test]
    fn test_remove_segment() {
        let temp_dir = tempdir().unwrap();
        let config = SegmentConfig::new(temp_dir.path());

        let mut manager = SegmentManager::new(config).unwrap();

        // 创建段
        let (id, path) = manager.create_segment().unwrap();
        assert!(path.exists());

        // 删除段
        let removed = manager.remove_segment(id).unwrap();
        assert!(removed.is_some());
        assert!(!path.exists());
    }
}
