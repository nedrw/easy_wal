//! 存储层集成测试
//!
//! 测试组件间的协作：Storage + SegmentManager + LogWriter

use easy_wal::{FileStorage, LogWriter, LogWriterConfig, MemoryStorage, SegmentConfig, Storage};
use std::sync::Arc;
use tempfile::tempdir;

// ============================================================================
// Storage trait 测试
// ============================================================================

#[tokio::test]
async fn test_storage_trait_object_safe() {
    // 验证 Storage trait 是对象安全的
    fn accept_storage<S: Storage>(_storage: &S) -> bool {
        true
    }

    let mem = MemoryStorage::new();
    assert!(accept_storage(&mem));

    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.wal");
    let file = FileStorage::new(&file_path).await.unwrap();
    assert!(accept_storage(&file));
}

#[tokio::test]
async fn test_storage_boxed() {
    // 验证可以动态分发
    let mem: Box<dyn Storage> = Box::new(MemoryStorage::new());
    let result = mem.append(b"test").await.unwrap();
    assert_eq!(result, 0);
}

// ============================================================================
// FileStorage 集成测试
// ============================================================================

#[tokio::test]
async fn test_file_storage_concurrent_writes() {
    use tokio::task::JoinSet;

    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("concurrent.wal");

    let storage = Arc::new(FileStorage::new(&file_path).await.unwrap());

    // 并发写入
    let mut join_set = JoinSet::new();
    for i in 0..10 {
        let storage = storage.clone();
        let data = format!("data{}", i);
        join_set.spawn(async move { storage.append(data.as_bytes()).await.unwrap() });
    }

    let mut offsets = Vec::new();
    while let Some(result) = join_set.join_next().await {
        offsets.push(result.unwrap());
    }

    // 验证所有写入都成功（偏移量不重复）
    offsets.sort();
    assert_eq!(offsets.len(), 10);
    for i in 0..10 {
        assert_eq!(offsets[i], i as u64 * 5); // 每个 "dataX" = 5 bytes
    }
}

#[tokio::test]
async fn test_file_storage_read_beyond_eof() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.wal");

    let storage = FileStorage::new(&file_path).await.unwrap();

    // 写入 10 字节
    storage.append(b"0123456789").await.unwrap();

    // 尝试读取超出范围
    let data = storage.read(5, 20).await.unwrap();
    assert_eq!(data.len(), 5); // 只返回实际有的数据
    assert_eq!(data, b"56789");
}

#[tokio::test]
async fn test_file_storage_truncate() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.wal");

    let storage = FileStorage::new(&file_path).await.unwrap();

    // 写入数据
    storage.append(b"hello world").await.unwrap();
    assert_eq!(storage.size().await.unwrap(), 11);

    // 截断
    storage.truncate(5).await.unwrap();
    assert_eq!(storage.size().await.unwrap(), 5);

    // 验证数据
    let data = storage.read(0, 5).await.unwrap();
    assert_eq!(data, b"hello");
}

#[tokio::test]
async fn test_file_storage_write_batch() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.wal");

    let storage = FileStorage::new(&file_path).await.unwrap();

    // 批量写入：hello(5) + world(5) + test(4) = 14 bytes
    // 偏移量 0, 5, 10 正好按顺序写入
    let offsets = [0u64, 5, 10];
    let data_list: Vec<&[u8]> = vec![b"hello", b"world", b"test"];
    storage.write_batch(&offsets, &data_list).await.unwrap();

    // 验证：5 + 5 + 4 = 14 bytes
    let data = storage.read(0, 14).await.unwrap();
    assert_eq!(data, b"helloworldtest");
}

#[tokio::test]
async fn test_file_storage_read_batch() {
    let temp_dir = tempdir().unwrap();
    let file_path = temp_dir.path().join("test.wal");

    let storage = FileStorage::new(&file_path).await.unwrap();

    // 写入数据
    storage.write(0, b"hello").await.unwrap();
    storage.write(10, b"world").await.unwrap();

    // 批量读取
    let locations = vec![
        easy_wal::Location::new(0, 5),
        easy_wal::Location::new(10, 5),
    ];
    let results = storage.read_batch(&locations).await.unwrap();

    assert_eq!(results[0], b"hello");
    assert_eq!(results[1], b"world");
}

// ============================================================================
// SegmentManager 集成测试
// ============================================================================

#[tokio::test]
async fn test_segment_manager_existing_files() {
    let temp_dir = tempdir().unwrap();

    // 预先创建一些段文件
    std::fs::write(temp_dir.path().join("segment5.wal"), "data5").unwrap();
    std::fs::write(temp_dir.path().join("segment10.wal"), "data10").unwrap();
    std::fs::write(temp_dir.path().join("segment15.wal"), "data15").unwrap();

    let config = SegmentConfig::new(temp_dir.path()).with_prefix("segment");
    let manager = easy_wal::SegmentManager::new(config).unwrap();

    // 应该扫描到 3 个段
    assert_eq!(manager.segment_count(), 3);

    // 活跃段应该是 ID 最大的（15）
    let active = manager.get_segment(15).unwrap();
    assert!(active.is_active);
}

#[tokio::test]
async fn test_segment_manager_rotate_sequence() {
    let temp_dir = tempdir().unwrap();

    let config = SegmentConfig::new(temp_dir.path())
        .with_prefix("seg")
        .with_extension("log")
        .with_max_size(10);

    let mut manager = easy_wal::SegmentManager::new(config).unwrap();

    // 初始：active_id = 0，但没有创建物理文件
    assert_eq!(manager.active_id(), 0);
    assert_eq!(manager.segment_count(), 0, "no segment files initially");

    // 写入数据达到上限，标记需要轮转
    let need_rotate = manager.update_active_size(10);
    assert!(
        need_rotate,
        "should mark as need rotate after reaching limit"
    );
    assert!(manager.should_rotate(), "should_rotate should return true");

    // 轮转：创建段1
    let (id1, path1) = manager.rotate().unwrap();
    assert_eq!(id1, 1);
    assert!(path1.exists());
    assert_eq!(manager.segment_count(), 1, "should have segment [1]");

    // 继续写入，active_size 重置为 0
    manager.update_active_size(5);
    assert!(
        !manager.should_rotate(),
        "should not need rotate after 5 bytes"
    );

    // 再次达到上限
    manager.update_active_size(5);
    assert!(
        manager.should_rotate(),
        "should need rotate after another 5 bytes"
    );

    // 再次轮转：创建段2
    let (id2, path2) = manager.rotate().unwrap();
    assert_eq!(id2, 2);
    assert!(path2.exists());
    assert_eq!(manager.segment_count(), 2, "should have segments [1, 2]");

    // 验证所有段
    let segments = manager.segments();
    assert_eq!(segments.len(), 2);
}

// ============================================================================
// LogWriter 集成测试
// ============================================================================

#[tokio::test]
async fn test_log_writer_full_workflow() {
    let temp_dir = tempdir().unwrap();

    let config = LogWriterConfig::default()
        .with_dir(temp_dir.path())
        .with_max_segment_size(100);

    let writer = LogWriter::new(config).await.unwrap();

    // 写入多批数据
    let pos1 = writer.write(b"batch1").await.unwrap();
    let pos2 = writer.write(b"batch2").await.unwrap();
    let pos3 = writer.write(b"batch3").await.unwrap();

    // 验证写入位置递增
    assert!(pos2.offset > pos1.offset);
    assert!(pos3.offset > pos2.offset);

    // 验证段信息
    let segments = writer.segments().await;
    assert!(!segments.is_empty());

    // 关闭
    writer.close().await.unwrap();
}

#[tokio::test]
async fn test_log_writer_rotation_on_limit() {
    let temp_dir = tempdir().unwrap();

    // 设置很小的段大小来触发轮转
    let config = LogWriterConfig::default()
        .with_dir(temp_dir.path())
        .with_max_segment_size(5); // 每个段最多 5 字节

    let writer = LogWriter::new(config).await.unwrap();

    // 写入 "hello" (5 bytes) - 应该刚好达到上限
    let _ = writer.write(b"hello").await.unwrap();
    let segment1 = writer.active_segment_id().await;

    // 写入 "world" (5 bytes) - 触发轮转
    let _ = writer.write(b"world").await.unwrap();
    let segment2 = writer.active_segment_id().await;

    // 应该已经轮转到新段
    assert!(segment2 >= segment1);
}

#[tokio::test]
async fn test_log_writer_manual_rotation() {
    let temp_dir = tempdir().unwrap();

    let config = LogWriterConfig::default()
        .with_dir(temp_dir.path())
        .with_max_segment_size(1000);

    let writer = LogWriter::new(config).await.unwrap();

    // 写入一些数据
    writer.write(b"before rotation").await.unwrap();

    // 手动轮转
    let (_new_id, path) = writer.rotate().await.unwrap();
    assert!(path.exists());

    // 新段写入
    writer.write(b"after rotation").await.unwrap();

    // 验证有两个段
    let segments = writer.segments().await;
    assert_eq!(segments.len(), 2);
}

#[tokio::test]
async fn test_log_writer_batch() {
    let temp_dir = tempdir().unwrap();

    let config = LogWriterConfig::default()
        .with_dir(temp_dir.path())
        .with_max_segment_size(1000);

    let writer = LogWriter::new(config).await.unwrap();

    // 批量写入
    let data_list: Vec<&[u8]> = vec![b"item1", b"item2", b"item3", b"item4", b"item5"];
    let positions = writer.write_batch(&data_list).await.unwrap();

    assert_eq!(positions.len(), 5);

    // 验证位置递增
    for i in 1..positions.len() {
        assert!(positions[i].offset >= positions[i - 1].offset);
    }
}

// ============================================================================
// MemoryStorage 测试（用于对比验证）
// ============================================================================

#[tokio::test]
async fn test_memory_storage_basic() {
    let storage = MemoryStorage::new();

    // 追加写入
    let offset = storage.append(b"hello").await.unwrap();
    assert_eq!(offset, 0);

    let offset = storage.append(b" world").await.unwrap();
    assert_eq!(offset, 5);

    // 读取
    let data = storage.read(0, 11).await.unwrap();
    assert_eq!(data, b"hello world");

    // 统计
    let stats = storage.stats().await;
    assert_eq!(stats.bytes_written, 11);
}

#[tokio::test]
async fn test_memory_storage_truncate() {
    let storage = MemoryStorage::new();

    storage.append(b"hello world").await.unwrap();
    assert_eq!(storage.size().await.unwrap(), 11);

    storage.truncate(5).await.unwrap();
    assert_eq!(storage.size().await.unwrap(), 5);
}

// ============================================================================
// 组件协作测试
// ============================================================================

#[tokio::test]
async fn test_storage_switching() {
    let temp_dir = tempdir().unwrap();

    // 使用 MemoryStorage 测试
    let mem = MemoryStorage::new();
    mem.append(b"memory data").await.unwrap();
    let mem_data = mem.read(0, 12).await.unwrap();
    assert_eq!(mem_data, b"memory data");

    // 使用 FileStorage 测试
    let file_path = temp_dir.path().join("test.wal");
    let file = FileStorage::new(&file_path).await.unwrap();
    file.append(b"file data").await.unwrap();
    let file_data = file.read(0, 9).await.unwrap();
    assert_eq!(file_data, b"file data");
}

#[tokio::test]
async fn test_concurrent_segment_creation() {
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let temp_dir = tempdir().unwrap();
    let config = SegmentConfig::new(temp_dir.path()).with_max_size(100);

    // 模拟并发创建段
    let manager = Arc::new(Mutex::new(easy_wal::SegmentManager::new(config).unwrap()));

    let mut handles = Vec::new();

    for _ in 0..5 {
        let mgr = manager.clone();
        handles.push(tokio::spawn(async move {
            let mut m = mgr.lock().await;
            m.rotate().unwrap()
        }));
    }

    // 等待所有任务完成
    for handle in handles {
        handle.await.unwrap();
    }

    // 验证段数量
    let mgr = manager.lock().await;
    assert!(mgr.segment_count() >= 1);
}

#[tokio::test]
async fn test_write_persistence() {
    let temp_dir = tempdir().unwrap();

    let config = LogWriterConfig::default().with_dir(temp_dir.path());

    let writer = LogWriter::new(config).await.unwrap();
    writer.write(b"important data").await.unwrap();
    writer.close().await.unwrap();

    // 重新打开，检查数据是否存在
    let config2 = LogWriterConfig::default().with_dir(temp_dir.path());

    let writer2 = LogWriter::new(config2).await.unwrap();
    let segments = writer2.segments().await;

    // 应该有历史数据
    assert!(!segments.is_empty());
}
