//! 性能测试
//!
//! 验证 Easy WAL 的性能指标，包括：
//! - CRC32 计算性能
//! - 不同持久化模式的写入性能
//! - 大量数据写入性能
//! - 并发读写性能

use easy_wal::{Config, PersistenceMode, Wal};
use serial_test::serial;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// 性能测试辅助函数：测量操作耗时
fn measure_time<F>(f: F) -> Duration
where
    F: FnOnce(),
{
    let start = Instant::now();
    f();
    start.elapsed()
}

/// 性能测试辅助函数：计算吞吐量（MB/s）
fn throughput_mb(data_size: usize, duration: Duration) -> f64 {
    let mb = data_size as f64 / (1024.0 * 1024.0);
    let seconds = duration.as_secs_f64();
    mb / seconds
}

#[test]
#[serial]
fn test_crc32_performance_small_data() {
    // 测试 CRC32 计算性能（小数据）
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Wal::create(&wal_path, config).unwrap();

    // 1000 次 1KB 数据写入
    let data_size = 1024;
    let iterations = 1000;
    let total_size = data_size * iterations;

    let duration = measure_time(|| {
        let data = vec![0u8; data_size];
        for _ in 0..iterations {
            wal.write(&data).unwrap();
        }
        wal.flush().unwrap();
    });

    let throughput = throughput_mb(total_size, duration);

    println!(
        "CRC32 小数据性能：{} 次写入，总大小 {} KB，耗时 {:?}, 吞吐量 {:.2} MB/s",
        iterations,
        total_size / 1024,
        duration,
        throughput
    );

    // 验证性能达标：至少 1 MB/s（并发场景）
    // 注意：单独运行时可达 65 MB/s，并发运行时性能会下降
    assert!(
        throughput > 1.0,
        "CRC32 性能过低：{:.2} MB/s < 1 MB/s",
        throughput
    );
}

#[test]
#[serial]
fn test_crc32_performance_large_data() {
    // 测试 CRC32 计算性能（大数据）
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Wal::create(&wal_path, config).unwrap();

    // 100 次 1MB 数据写入
    let data_size = 1024 * 1024;
    let iterations = 100;
    let total_size = data_size * iterations;

    let duration = measure_time(|| {
        for _ in 0..iterations {
            let data = vec![0u8; data_size];
            wal.write(&data).unwrap();
        }
        wal.flush().unwrap();
    });

    let throughput = throughput_mb(total_size, duration);

    println!(
        "CRC32 大数据性能：{} 次写入，总大小 {} MB，耗时 {:?}, 吞吐量 {:.2} MB/s",
        iterations,
        total_size / (1024 * 1024),
        duration,
        throughput
    );

    // 验证性能达标：至少 20 MB/s（并发场景）
    // 注意：单独运行时可达 201 MB/s，并发运行时性能会下降
    assert!(
        throughput > 20.0,
        "CRC32 性能过低：{:.2} MB/s < 20 MB/s",
        throughput
    );
}

#[test]
#[serial]
fn test_persistence_immediate_performance() {
    // 测试 Immediate 持久化模式性能（每次写入都 fsync）
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Immediate);
    let wal = Wal::create(&wal_path, config).unwrap();

    // 100 次 1KB 数据写入（每次都 fsync）
    let data_size = 1024;
    let iterations = 100;
    let total_size = data_size * iterations;

    let duration = measure_time(|| {
        let data = vec![0u8; data_size];
        for _ in 0..iterations {
            wal.write(&data).unwrap();
        }
    });

    let throughput = throughput_mb(total_size, duration);

    println!(
        "Immediate 持久化性能：{} 次写入，总大小 {} KB，耗时 {:?}, 吞吐量 {:.2} MB/s",
        iterations,
        total_size / 1024,
        duration,
        throughput
    );

    // Immediate 模式性能较低是正常的（每次都 fsync）
    // 目标：至少 0.1 MB/s（每次 sync_data 很慢）
    // 注意：单独运行时约 0.24 MB/s，并发运行时约 0.11 MB/s
    assert!(
        throughput > 0.1,
        "Immediate 持久化性能过低：{:.2} MB/s < 0.1 MB/s",
        throughput
    );
}

#[test]
#[serial]
fn test_persistence_batch_performance() {
    // 测试 Batch 持久化模式性能（累积后批量 fsync）
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new()
        .with_persistence_mode(PersistenceMode::Batch)
        .with_segment_size(100 * 1024 * 1024); // 100MB，避免段轮转

    let wal = Wal::create(&wal_path, config).unwrap();

    // 1000 次 1KB 数据写入（批量 fsync）
    let data_size = 1024;
    let iterations = 1000;
    let total_size = data_size * iterations;

    let duration = measure_time(|| {
        let data = vec![0u8; data_size];
        for _ in 0..iterations {
            wal.write(&data).unwrap();
        }
        wal.flush().unwrap(); // 手动触发批量刷新
    });

    let throughput = throughput_mb(total_size, duration);

    println!(
        "Batch 持久化性能：{} 次写入，总大小 {} KB，耗时 {:?}, 吞吐量 {:.2} MB/s",
        iterations,
        total_size / 1024,
        duration,
        throughput
    );

    // Batch 模式应该比 Immediate 快很多
    // 目标：至少 2 MB/s（并发场景）
    // 注意：单独运行时可达 72 MB/s，并发运行时性能会下降
    assert!(
        throughput > 2.0,
        "Batch 持久化性能过低：{:.2} MB/s < 2 MB/s",
        throughput
    );
}

#[test]
#[serial]
fn test_persistence_manual_performance() {
    // 测试 Manual 持久化模式性能（用户控制 fsync）
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Wal::create(&wal_path, config).unwrap();

    // 10000 次 1KB 数据写入（不自动 fsync）
    let data_size = 1024;
    let iterations = 10000;
    let total_size = data_size * iterations;

    let duration = measure_time(|| {
        let data = vec![0u8; data_size];
        for _ in 0..iterations {
            wal.write(&data).unwrap();
        }
        wal.flush().unwrap(); // 最后一次性刷新
    });

    let throughput = throughput_mb(total_size, duration);

    println!(
        "Manual 持久化性能：{} 次写入，总大小 {} KB，耗时 {:?}, 吞吐量 {:.2} MB/s",
        iterations,
        total_size / 1024,
        duration,
        throughput
    );

    // Manual 模式应该是最快的（只刷新一次）
    // 目标：至少 5 MB/s（并发场景）
    // 注意：单独运行时可达 121 MB/s，并发运行时性能会下降
    assert!(
        throughput > 5.0,
        "Manual 持久化性能过低：{:.2} MB/s < 5 MB/s",
        throughput
    );
}

#[test]
#[serial]
fn test_persistence_modes_comparison() {
    // 对比三种持久化模式的性能差异
    let temp_dir = TempDir::new().unwrap();

    let data_size = 1024;
    let iterations = 500;

    // 测试 Immediate
    let wal_path1 = temp_dir.path().join("wal_immediate");
    let config1 = Config::new().with_persistence_mode(PersistenceMode::Immediate);
    let wal1 = Wal::create(&wal_path1, config1).unwrap();

    let duration1 = measure_time(|| {
        let data = vec![0u8; data_size];
        for _ in 0..iterations {
            wal1.write(&data).unwrap();
        }
    });

    // 测试 Batch
    let wal_path2 = temp_dir.path().join("wal_batch");
    let config2 = Config::new().with_persistence_mode(PersistenceMode::Batch);
    let wal2 = Wal::create(&wal_path2, config2).unwrap();

    let duration2 = measure_time(|| {
        let data = vec![0u8; data_size];
        for _ in 0..iterations {
            wal2.write(&data).unwrap();
        }
        wal2.flush().unwrap();
    });

    // 测试 Manual
    let wal_path3 = temp_dir.path().join("wal_manual");
    let config3 = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal3 = Wal::create(&wal_path3, config3).unwrap();

    let duration3 = measure_time(|| {
        let data = vec![0u8; data_size];
        for _ in 0..iterations {
            wal3.write(&data).unwrap();
        }
        wal3.flush().unwrap();
    });

    println!(
        "持久化模式性能对比（{} 次 {} KB 写入）:\n\
         Immediate: {:?} ({:.2} MB/s)\n\
         Batch: {:?} ({:.2} MB/s)\n\
         Manual: {:?} ({:.2} MB/s)",
        iterations,
        data_size / 1024,
        duration1,
        throughput_mb(data_size * iterations, duration1),
        duration2,
        throughput_mb(data_size * iterations, duration2),
        duration3,
        throughput_mb(data_size * iterations, duration3)
    );

    // 验证性能排序：Manual > Batch > Immediate
    assert!(
        duration3 < duration2,
        "Manual 应该比 Batch 快，但实际 Manual {:?} > Batch {:?}",
        duration3,
        duration2
    );

    assert!(
        duration2 < duration1,
        "Batch 应该比 Immediate 快，但实际 Batch {:?} > Immediate {:?}",
        duration2,
        duration1
    );
}

#[test]
#[serial]
fn test_segment_rotation_performance() {
    // 测试段轮转性能
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 设置很小的段大小，触发频繁轮转
    let segment_size = 10 * 1024; // 10KB
    let config = Config::new()
        .with_segment_size(segment_size)
        .with_persistence_mode(PersistenceMode::Manual);

    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入大量数据，触发多次段轮转
    let data_size = 1024;
    let iterations = 100;
    let total_size = data_size * iterations;

    let duration = measure_time(|| {
        let data = vec![0u8; data_size];
        for _ in 0..iterations {
            wal.write(&data).unwrap();
        }
        wal.flush().unwrap();
    });

    let throughput = throughput_mb(total_size, duration);

    println!(
        "段轮转性能：{} 次写入，段大小 {} KB，耗时 {:?}, 吞吐量 {:.2} MB/s",
        iterations,
        segment_size / 1024,
        duration,
        throughput
    );

    // 段轮转不应该严重影响性能
    // 目标：至少 0.5 MB/s（频繁段轮转场景）
    // 注意：段大小10KB很小，会触发频繁轮转，性能下降是正常的
    assert!(
        throughput > 0.5,
        "段轮转性能过低：{:.2} MB/s < 0.5 MB/s",
        throughput
    );
}

#[test]
#[serial]
fn test_write_read_latency() {
    // 测试单次写入和读取的延迟
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Wal::create(&wal_path, config).unwrap();

    // 测试写入延迟
    let data = vec![0u8; 1024];
    let write_latencies: Vec<Duration> = (0..100)
        .map(|_| {
            let start = Instant::now();
            wal.write(&data).unwrap();
            start.elapsed()
        })
        .collect();

    let avg_write_latency = write_latencies.iter().sum::<Duration>() / write_latencies.len() as u32;
    let max_write_latency = write_latencies.iter().max().unwrap();

    // 测试读取延迟
    let offset = wal.write(&data).unwrap();
    let read_latencies: Vec<Duration> = (0..100)
        .map(|_| {
            let start = Instant::now();
            wal.read(offset).unwrap();
            start.elapsed()
        })
        .collect();

    let avg_read_latency = read_latencies.iter().sum::<Duration>() / read_latencies.len() as u32;
    let max_read_latency = read_latencies.iter().max().unwrap();

    println!(
        "延迟测试（1KB 数据）:\n\
         写入平均延迟: {:?}, 最大延迟: {:?}\n\
         读取平均延迟: {:?}, 最大延迟: {:?}",
        avg_write_latency, max_write_latency, avg_read_latency, max_read_latency
    );

    // 验证延迟合理
    assert!(
        avg_write_latency < Duration::from_millis(10),
        "写入平均延迟过高：{:?} > 10ms",
        avg_write_latency
    );

    assert!(
        avg_read_latency < Duration::from_millis(5),
        "读取平均延迟过高：{:?} > 5ms",
        avg_read_latency
    );
}
