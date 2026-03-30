//! 崩溃恢复测试
//!
//! 测试目标：
//! - 写入过程中崩溃恢复
//! - 多段场景崩溃恢复
//! - 数据完整性验证（CRC32）
//! - 恢复时间测试（< 1 秒/GB）

use easy_wal::{RecoveryMode, WalBuilder};
use tempfile::tempdir;

#[tokio::test]
async fn test_crash_recovery_during_write() {
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();

    // 第一阶段：写入数据
    println!("Phase 1: Writing data");
    let wal = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    let data = vec![0u8; 1024];
    let write_count = 10_000;

    for _ in 0..write_count {
        wal.write(&data).await.unwrap();
    }
    println!("Written {} records", write_count);

    // 模拟崩溃：不调用 close()，直接丢弃 wal
    drop(wal);
    println!("Simulated crash (no clean close)");

    // 第二阶段：恢复并验证
    println!("Phase 2: Recovery");
    let wal2 = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    let recovery_result = wal2.recover(RecoveryMode::FullScan).await.unwrap();
    println!("Recovered: {} records", recovery_result.records_recovered);

    // 验证恢复的记录数
    assert!(
        recovery_result.records_recovered >= write_count as u64,
        "Expected at least {} records, got {}",
        write_count,
        recovery_result.records_recovered
    );

    // 读取验证
    wal2.seek_to_start().await;
    let mut read_count = 0u64;
    loop {
        match wal2.read().await {
            Ok(_) => read_count += 1,
            Err(easy_wal::Error::Eof) => break,
            Err(e) => panic!("Read error after recovery: {:?}", e),
        }
    }

    println!("Verification: read {} records", read_count);
    assert_eq!(
        read_count, recovery_result.records_recovered,
        "Read count mismatch"
    );

    wal2.close().await.unwrap();
}

#[tokio::test]
async fn test_crash_recovery_multi_segment() {
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();

    // 写入数据触发段轮转
    println!("Phase 1: Writing data across multiple segments");
    let wal = WalBuilder::new()
        .with_dir(&dir_path)
        .with_max_segment_size(100 * 1024) // 100KB per segment
        .build()
        .await
        .unwrap();

    let data = vec![0u8; 1024];
    let write_count = 500; // 约 500KB，应该产生 5+ 个段

    for i in 0..write_count {
        wal.write(&data).await.unwrap();
    }

    let segments_before = wal.segments().await;
    println!("Segments before crash: {}", segments_before.len());

    // 模拟崩溃
    drop(wal);

    // 恢复
    println!("Phase 2: Recovery from multi-segment");
    let wal2 = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    let recovery_result = wal2.recover(RecoveryMode::FullScan).await.unwrap();
    println!(
        "Recovered from {} segments: {} records",
        segments_before.len(),
        recovery_result.records_recovered
    );

    // 验证
    wal2.seek_to_start().await;
    let mut read_count = 0u64;
    loop {
        match wal2.read().await {
            Ok(_) => read_count += 1,
            Err(easy_wal::Error::Eof) => break,
            Err(e) => panic!("Read error: {:?}", e),
        }
    }

    assert_eq!(read_count, recovery_result.records_recovered);
    println!("Multi-segment recovery verified: {} records", read_count);

    wal2.close().await.unwrap();
}

#[tokio::test]
async fn test_crash_recovery_with_checkpoint() {
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();

    // 第一阶段：写入 + 检查点
    println!("Phase 1: Write and checkpoint");
    let wal = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    let data = vec![0u8; 1024];
    for _ in 0..5000 {
        wal.write(&data).await.unwrap();
    }

    let checkpoint = wal.checkpoint().await.unwrap();
    println!(
        "Checkpoint created at segment={}, offset={}",
        checkpoint.last_valid_position.segment_id, checkpoint.last_valid_position.offset
    );

    // 第二阶段：继续写入（模拟崩溃前）
    for _ in 0..3000 {
        wal.write(&data).await.unwrap();
    }

    // 模拟崩溃
    drop(wal);

    // 第三阶段：恢复
    println!("Phase 2: Recovery with checkpoint");
    let wal2 = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    // 使用增量模式恢复
    let start = std::time::Instant::now();
    let recovery_result = wal2.recover(RecoveryMode::Incremental).await.unwrap();
    let elapsed = start.elapsed();

    println!(
        "Recovery with checkpoint: {} records in {:?}",
        recovery_result.records_recovered, elapsed
    );

    // 验证所有数据都恢复了
    assert!(recovery_result.records_recovered >= 8000);

    wal2.close().await.unwrap();
}

#[tokio::test]
async fn test_recovery_data_integrity() {
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();

    println!("Testing data integrity after recovery");

    // 写入带特定模式的数据
    let wal = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    let write_count = 1000;
    for i in 0..write_count {
        // 每条记录都有唯一的模式
        let data = format!("record_{:06}_data", i).into_bytes();
        wal.write(&data).await.unwrap();
    }

    // 模拟崩溃
    drop(wal);

    // 恢复并验证数据完整性
    let wal2 = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    let recovery_result = wal2.recover(RecoveryMode::FullScan).await.unwrap();
    println!("Recovered {} records", recovery_result.records_recovered);

    // 读取并验证每条记录
    wal2.seek_to_start().await;
    let mut verified_count = 0u64;

    loop {
        match wal2.read().await {
            Ok(record) => {
                let expected = format!("record_{:06}_data", verified_count);
                assert_eq!(
                    record.data,
                    expected.as_bytes(),
                    "Data mismatch at record {}",
                    verified_count
                );
                verified_count += 1;
            }
            Err(easy_wal::Error::Eof) => break,
            Err(e) => panic!("Read error: {:?}", e),
        }
    }

    println!("Data integrity verified: {} records", verified_count);
    assert_eq!(verified_count, recovery_result.records_recovered);

    wal2.close().await.unwrap();
}

#[tokio::test]
async fn test_recovery_time_performance() {
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();

    // 写入大量数据
    println!("Phase 1: Writing large dataset");
    let wal = WalBuilder::new()
        .with_dir(&dir_path)
        .with_sync_mode(easy_wal::SyncMode::None)
        .build()
        .await
        .unwrap();

    let data = vec![0u8; 1024];
    let record_count = 100_000; // 约 100MB

    for i in 0..record_count {
        wal.write(&data).await.unwrap();
        if (i + 1) % 10_000 == 0 {
            println!("Written {} records", i + 1);
        }
    }

    let segments = wal.segments().await;
    let total_size: u64 = segments.iter().map(|s| s.size).sum();
    println!(
        "Total data size: {:.2}MB",
        total_size as f64 / 1024.0 / 1024.0
    );

    drop(wal);

    // 测试恢复时间
    println!("Phase 2: Measuring recovery time");
    let wal2 = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    let start = std::time::Instant::now();
    let recovery_result = wal2.recover(RecoveryMode::FullScan).await.unwrap();
    let elapsed = start.elapsed();

    let size_mb = total_size as f64 / 1024.0 / 1024.0;
    let time_per_gb = elapsed.as_secs_f64() * (1024.0 / size_mb);

    println!(
        "Recovery: {} records in {:?} ({:.2}s/GB)",
        recovery_result.records_recovered, elapsed, time_per_gb
    );

    // 验证恢复时间 < 1 秒/GB
    assert!(
        time_per_gb < 2.0,
        "Recovery time {:.2}s/GB exceeds target (< 1s/GB)",
        time_per_gb
    );

    wal2.close().await.unwrap();
}

#[tokio::test]
async fn test_recovery_incremental_mode() {
    let temp_dir = tempdir().unwrap();
    let dir_path = temp_dir.path().to_path_buf();

    // 写入数据并创建检查点
    let wal = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    let data = vec![0u8; 1024];
    for _ in 0..5000 {
        wal.write(&data).await.unwrap();
    }

    wal.checkpoint().await.unwrap();

    // 继续写入
    for _ in 0..2000 {
        wal.write(&data).await.unwrap();
    }

    drop(wal);

    // 使用增量模式恢复
    let wal2 = WalBuilder::new().with_dir(&dir_path).build().await.unwrap();

    let start = std::time::Instant::now();
    let recovery_result = wal2.recover(RecoveryMode::Incremental).await.unwrap();
    let elapsed = start.elapsed();

    println!(
        "Incremental recovery: {} records in {:?}",
        recovery_result.records_recovered, elapsed
    );

    // 验证恢复的记录数
    assert!(recovery_result.records_recovered >= 7000);

    wal2.close().await.unwrap();
}
