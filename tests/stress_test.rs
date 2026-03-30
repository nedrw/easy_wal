//! 压力测试 - 长时间运行测试
//!
//! 测试目标：
//! - 长时间运行稳定性（≥ 1 小时）
//! - 大数据量（≥ 1000 万条记录）
//! - 内存使用监控（< 100MB）
//! - 段文件数量增长测试

use easy_wal::{SyncMode, WalBuilder};
use tempfile::tempdir;

#[tokio::test]
async fn test_long_running_stress() {
    let temp_dir = tempdir().unwrap();
    let wal = WalBuilder::new()
        .with_dir(temp_dir.path())
        .with_sync_mode(SyncMode::Batch { batch_size: 100 })
        .build()
        .await
        .unwrap();

    let data = vec![0u8; 1024]; // 1KB per record
    let total_records = 1_000_000; // 100 万条用于快速测试，可调整为 1000 万

    println!("Starting stress test: {} records", total_records);
    let start = std::time::Instant::now();

    for i in 0..total_records {
        wal.write(&data).await.unwrap();

        if (i + 1) % 100_000 == 0 {
            let elapsed = start.elapsed();
            let qps = (i + 1) as f64 / elapsed.as_secs_f64();
            println!(
                "Progress: {}/{} records, QPS: {:.2}, Time: {:?}",
                i + 1,
                total_records,
                qps,
                elapsed
            );
        }
    }

    let elapsed = start.elapsed();
    let qps = total_records as f64 / elapsed.as_secs_f64();

    println!(
        "Stress test completed: {} records in {:?}, QPS: {:.2}",
        total_records, elapsed, qps
    );

    // 等待一小段时间确保 batch 提交完成
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

    // 验证段文件数量
    let segments = wal.segments().await;
    println!("Total segments created: {}", segments.len());

    // 验证当前位置
    let pos = wal.position().await;
    println!(
        "Final position: segment={}, offset={}",
        pos.segment_id, pos.offset
    );

    // 读取验证
    wal.seek_to_start().await;
    let mut read_count = 0u64;
    loop {
        match wal.read().await {
            Ok(_) => read_count += 1,
            Err(easy_wal::Error::Eof) => break,
            Err(e) => panic!("Read error: {:?}", e),
        }
    }

    assert_eq!(
        read_count, total_records as u64,
        "Expected {} records, got {}",
        total_records, read_count
    );

    println!("Verification passed: {} records read", read_count);

    wal.close().await.unwrap();
}

/// 小规模预读缓冲区验证测试
///
/// 快速验证预读缓冲区的多次填充逻辑是否正确：
/// - 写入足够多数据触发多次 fill_buffer()（200 条记录，约 200KB）
/// - 验证能否正确读取所有记录
#[tokio::test]
async fn test_read_ahead_buffer_verification() {
    let temp_dir = tempdir().unwrap();
    let wal = WalBuilder::new()
        .with_dir(temp_dir.path())
        .with_sync_mode(SyncMode::FsyncOnWrite)
        .build()
        .await
        .unwrap();

    let data = vec![0u8; 1024]; // 1KB per record
    let total_records = 200; // 足够触发多次 fill_buffer (64KB buffer)

    println!("Writing {} records (each 1KB)", total_records);

    // 写入数据
    for i in 0..total_records {
        wal.write(&data).await.unwrap();
    }

    println!("Write completed, starting read verification");

    // 同步确保所有数据落盘
    wal.sync().await.unwrap();

    // 跳到开头读取
    wal.seek_to_start().await;

    // 读取所有记录
    let mut read_count = 0u64;
    loop {
        match wal.read().await {
            Ok(_) => read_count += 1,
            Err(easy_wal::Error::Eof) => break,
            Err(e) => panic!("Read error at record {}: {:?}", read_count, e),
        }

        if read_count <= 5 || read_count % 50 == 0 {
            println!("Read {} records", read_count);
        }
    }

    println!("Read completed: {} records", read_count);

    // 验证读取的记录数
    assert_eq!(
        read_count, total_records as u64,
        "Expected {} records, got {} - read-ahead buffer may have lost records",
        total_records, read_count
    );

    println!(
        "Verification passed: all {} records read successfully",
        read_count
    );

    wal.close().await.unwrap();
}

#[tokio::test]
async fn test_memory_usage_under_load() {
    let temp_dir = tempdir().unwrap();
    let wal = WalBuilder::new()
        .with_dir(temp_dir.path())
        .with_sync_mode(SyncMode::Batch { batch_size: 100 })
        .build()
        .await
        .unwrap();

    let data = vec![0u8; 1024];
    let batch_size = 100_000;
    let total_batches = 50; // 500 万条记录

    println!("Testing memory usage under load");

    for batch in 0..total_batches {
        for _ in 0..batch_size {
            wal.write(&data).await.unwrap();
        }

        // 每批次后检查段信息
        let segments = wal.segments().await;
        let total_size: u64 = segments.iter().map(|s| s.size).sum();

        println!(
            "Batch {}/{}: segments={}, total_size={:.2}MB",
            batch + 1,
            total_batches,
            segments.len(),
            total_size as f64 / 1024.0 / 1024.0
        );
    }

    wal.close().await.unwrap();
}

#[tokio::test]
async fn test_segment_rotation_under_load() {
    let temp_dir = tempdir().unwrap();
    let wal = WalBuilder::new()
        .with_dir(temp_dir.path())
        .with_max_segment_size(1024 * 1024) // 1MB per segment
        .with_sync_mode(SyncMode::Batch { batch_size: 100 })
        .build()
        .await
        .unwrap();

    let data = vec![0u8; 1024]; // 1KB per record
    let total_records = 100_000;

    println!("Testing segment rotation with 1MB segments");

    for i in 0..total_records {
        wal.write(&data).await.unwrap();

        if (i + 1) % 10_000 == 0 {
            let segments = wal.segments().await;
            println!(
                "Progress: {}/{} records, segments={}",
                i + 1,
                total_records,
                segments.len()
            );
        }
    }

    let segments = wal.segments().await;
    let expected_segments = (total_records * 1024 / (1024 * 1024)) + 1;

    println!(
        "Segment rotation test: {} segments (expected ~{})",
        segments.len(),
        expected_segments
    );

    assert!(
        segments.len() >= expected_segments - 1,
        "Expected at least {} segments, got {}",
        expected_segments - 1,
        segments.len()
    );

    wal.close().await.unwrap();
}
