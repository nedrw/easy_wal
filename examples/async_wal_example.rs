//! AsyncWal 使用示例
//!
//! 展示如何使用异步 WAL 进行高效的数据读写操作

use easy_wal::{AsyncWal, Config, PersistenceMode};
use std::sync::Arc;
use tempfile::TempDir;
use tokio::task;

/// 示例 1: 基本 AsyncWal 用法
///
/// 展示创建、写入、读取、flush 和关闭的基本流程
#[tokio::main]
async fn basic_usage() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("async_wal");

    // 创建 AsyncWal（使用 Manual 模式，性能最优）
    let config = Config::new()
        .with_persistence_mode(PersistenceMode::Manual)
        .with_segment_size(1024 * 1024); // 1MB 段大小

    let wal = AsyncWal::create(&wal_path, config)
        .await
        .expect("Failed to create AsyncWal");

    println!("✅ AsyncWal created at: {}", wal_path.display());

    // 异步写入多条数据
    let data_items = vec![
        b"First async record".to_vec(),
        b"Second async record".to_vec(),
        b"Third async record".to_vec(),
    ];

    let mut offsets = vec![];
    for data in &data_items {
        let offset = wal.write(data).await.expect("Failed to write data");
        offsets.push(offset);
        println!("📝 Written at offset {}: {} bytes", offset, data.len());
    }

    // 手动 flush（Manual 模式需要显式调用）
    wal.flush().await.expect("Failed to flush");
    println!("💾 Data flushed to disk");

    // 异步读取数据
    for (i, offset) in offsets.iter().enumerate() {
        let data = wal.read(*offset).await.expect("Failed to read data");
        println!(
            "📖 Read from offset {}: {:?}",
            i,
            String::from_utf8_lossy(&data)
        );
    }

    // 关闭 WAL
    wal.close().await.expect("Failed to close");
    println!("🔒 AsyncWal closed");
}

/// 示例 2: 持久化模式对比
///
/// 展示 Immediate、Batch 和 Manual 三种持久化模式的差异
#[tokio::main]
async fn persistence_modes() {
    let temp_dir = TempDir::new().unwrap();

    // Immediate 模式：每次写入都自动 sync，最安全但最慢
    let immediate_path = temp_dir.path().join("immediate_wal");
    let immediate_config = Config::new().with_persistence_mode(PersistenceMode::Immediate);

    let immediate_wal = AsyncWal::create(&immediate_path, immediate_config)
        .await
        .unwrap();

    // 写入数据（每次写入都会自动 sync）
    immediate_wal.write(b"Critical data 1").await.unwrap();
    immediate_wal.write(b"Critical data 2").await.unwrap();
    println!("✅ Immediate mode: Data synced after each write");

    // Batch 模式：批量写入，手动 flush
    let batch_path = temp_dir.path().join("batch_wal");
    let batch_config = Config::new().with_persistence_mode(PersistenceMode::Batch);

    let batch_wal = AsyncWal::create(&batch_path, batch_config).await.unwrap();

    // 批量写入多条数据
    for i in 0..10 {
        batch_wal
            .write(format!("Batch data {}", i).as_bytes())
            .await
            .unwrap();
    }

    // 手动触发批量 flush
    batch_wal.flush().await.unwrap();
    println!("✅ Batch mode: Data flushed in batch");

    // Manual 模式：完全手动控制，性能最优
    let manual_path = temp_dir.path().join("manual_wal");
    let manual_config = Config::new().with_persistence_mode(PersistenceMode::Manual);

    let manual_wal = AsyncWal::create(&manual_path, manual_config).await.unwrap();

    // 写入大量数据，只在最后 flush
    for i in 0..100 {
        manual_wal
            .write(format!("Manual data {}", i).as_bytes())
            .await
            .unwrap();
    }

    // 只 flush 一次（性能最优）
    manual_wal.flush().await.unwrap();
    println!("✅ Manual mode: Best performance with single flush");
}

/// 示例 3: 异步并发写入
///
/// 展示多任务并发写入 AsyncWal 的场景
#[tokio::main]
async fn concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("concurrent_wal");

    let config = Config::new()
        .with_persistence_mode(PersistenceMode::Manual)
        .with_segment_size(10 * 1024 * 1024); // 10MB

    let wal = Arc::new(
        AsyncWal::create(&wal_path, config)
            .await
            .expect("Failed to create AsyncWal"),
    );

    println!("✅ AsyncWal created for concurrent writes");

    // 启动多个并发写入任务
    let mut tasks = vec![];

    for task_id in 0..5 {
        let wal_clone = Arc::clone(&wal);

        let task = task::spawn(async move {
            for i in 0..20 {
                let data = format!("Task {} - Record {}", task_id, i);
                let _offset = wal_clone.write(data.as_bytes()).await.unwrap();

                // 模拟一些处理时间
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        });

        tasks.push(task);
    }

    // 等待所有任务完成
    for task in tasks {
        task.await.expect("Task failed");
    }

    // 最终 flush
    wal.flush().await.expect("Failed to flush");

    println!("✅ Concurrent writes completed: {} tasks × 20 records", 5);
    println!("📊 Total segments: {}", wal.segment_count().await);
}

/// 示例 4: 重新打开已有的 AsyncWal
///
/// 展示如何持久化数据并在后续重新打开
#[tokio::main]
async fn reopen_wal() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("persistent_wal");

    // 第一次：创建并写入数据
    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);

    let wal1 = AsyncWal::create(&wal_path, config)
        .await
        .expect("Failed to create AsyncWal");

    let data1 = b"Persistent record 1";
    let data2 = b"Persistent record 2";

    let offset1 = wal1.write(data1).await.unwrap();
    let offset2 = wal1.write(data2).await.unwrap();

    wal1.flush().await.unwrap();
    wal1.close().await.unwrap();

    println!("✅ First session: Data written and closed");

    // 第二次：重新打开并读取数据
    let wal2 = AsyncWal::open(&wal_path)
        .await
        .expect("Failed to open AsyncWal");

    // 验证数据完整性
    let read1 = wal2.read(offset1).await.unwrap();
    let read2 = wal2.read(offset2).await.unwrap();

    assert_eq!(read1, data1);
    assert_eq!(read2, data2);

    println!("✅ Second session: Data verified successfully");

    // 继续追加新数据
    let data3 = b"New persistent record 3";
    let offset3 = wal2.write(data3).await.unwrap();
    wal2.flush().await.unwrap();

    let read3 = wal2.read(offset3).await.unwrap();
    assert_eq!(read3, data3);

    println!("✅ Data appended successfully");
}

/// 主函数：运行所有示例
fn main() {
    println!("=== AsyncWal Usage Examples ===\n");

    println!("📌 Example 1: Basic AsyncWal Usage");
    basic_usage();
    println!();

    println!("📌 Example 2: Persistence Modes Comparison");
    persistence_modes();
    println!();

    println!("📌 Example 3: Concurrent Writes");
    concurrent_writes();
    println!();

    println!("📌 Example 4: Reopen AsyncWal");
    reopen_wal();
    println!();

    println!("=== All Examples Completed ===");
}
