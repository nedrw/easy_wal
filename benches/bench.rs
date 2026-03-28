/// Easy WAL 性能基准测试
///
/// 验证目标: 10万+ QPS
///
/// 运行方式:
///   cargo bench
///
/// 测试场景:
/// - 写入吞吐量 (Write Throughput)
/// - 读取吞吐量 (Read Throughput)
/// - 批量写入/读取 (Batch Operations)
/// - 崩溃恢复 (Crash Recovery)
/// - 并发写入 (Concurrent Write)
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use easy_wal::{RecoveryMode, SyncMode, WalBuilder};
use std::hint::black_box;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

// ============================================================
// 辅助函数
// ============================================================

fn create_wal(
    rt: &tokio::runtime::Runtime,
    dir: &std::path::Path,
    sync: SyncMode,
) -> easy_wal::WalManager {
    rt.block_on(async {
        WalBuilder::new()
            .with_dir(dir)
            .with_sync_mode(sync)
            .build()
            .await
            .unwrap()
    })
}

// ============================================================
// 写入基准测试
// ============================================================

fn bench_write_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("write_throughput");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    // 1KB sync write
    group.bench_function("write_1kb_sync", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let wal = create_wal(&rt, temp_dir.path(), SyncMode::FsyncOnWrite);
            let data = vec![0u8; 1024];
            for _ in 0..1000 {
                rt.block_on(wal.write(&data)).unwrap();
            }
            rt.block_on(wal.close()).unwrap();
        });
    });

    // 1KB no sync write
    group.bench_function("write_1kb_nosync", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let wal = create_wal(&rt, temp_dir.path(), SyncMode::None);
            let data = vec![0u8; 1024];
            for _ in 0..1000 {
                rt.block_on(wal.write(&data)).unwrap();
            }
            rt.block_on(wal.close()).unwrap();
        });
    });

    // 100B sync write
    group.bench_function("write_100b_sync", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let wal = create_wal(&rt, temp_dir.path(), SyncMode::FsyncOnWrite);
            let data = vec![0u8; 100];
            for _ in 0..1000 {
                rt.block_on(wal.write(&data)).unwrap();
            }
            rt.block_on(wal.close()).unwrap();
        });
    });

    group.throughput(Throughput::Elements(1000));
}

// ============================================================
// 读取基准测试
// ============================================================

fn bench_read_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("read_throughput");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    group.bench_function("read_1kb_1k_records", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let wal = rt.block_on(async {
                let wal = WalBuilder::new()
                    .with_dir(temp_dir.path())
                    .with_sync_mode(SyncMode::None)
                    .build()
                    .await
                    .unwrap();
                let data = vec![0u8; 1024];
                for _ in 0..1000 {
                    wal.write(&data).await.unwrap();
                }
                wal.seek_to_start().await;
                wal
            });

            let mut count = 0u32;
            loop {
                match rt.block_on(wal.read()) {
                    Ok(_) => count += 1,
                    Err(easy_wal::Error::Eof) => break,
                    Err(e) => panic!("Read error: {:?}", e),
                }
            }
            assert_eq!(count, 1000);
            rt.block_on(wal.close()).unwrap();
        });
    });

    group.throughput(Throughput::Elements(10000));
}

// ============================================================
// 批量写入基准测试
// ============================================================

fn bench_batch_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("batch_write");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    group.bench_function("batch_10_write_1kb", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let wal = create_wal(&rt, temp_dir.path(), SyncMode::None);
            let data = vec![0u8; 1024];
            for _ in 0..100 {
                let batch: Vec<&[u8]> = std::iter::repeat(&*data).take(10).collect();
                rt.block_on(wal.write_batch(&batch)).unwrap();
            }
            rt.block_on(wal.close()).unwrap();
        });
    });

    group.bench_function("batch_100_write_100b", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let wal = create_wal(&rt, temp_dir.path(), SyncMode::None);
            let data = vec![0u8; 100];
            for _ in 0..10 {
                let batch: Vec<&[u8]> = std::iter::repeat(&*data).take(100).collect();
                rt.block_on(wal.write_batch(&batch)).unwrap();
            }
            rt.block_on(wal.close()).unwrap();
        });
    });

    group.throughput(Throughput::Elements(1000));
}

// ============================================================
// 批量读取基准测试
// ============================================================

fn bench_batch_read(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("batch_read");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    group.bench_function("batch_read_100_records", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let wal = rt.block_on(async {
                let wal = WalBuilder::new()
                    .with_dir(temp_dir.path())
                    .with_sync_mode(SyncMode::None)
                    .build()
                    .await
                    .unwrap();
                let data = vec![0u8; 1024];
                for _ in 0..1000 {
                    wal.write(&data).await.unwrap();
                }
                wal.seek_to_start().await;
                wal
            });

            let mut total = 0u32;
            loop {
                let records = rt.block_on(wal.read_batch(100)).unwrap();
                if records.is_empty() {
                    break;
                }
                total += records.len() as u32;
            }
            assert_eq!(total, 1000);
            rt.block_on(wal.close()).unwrap();
        });
    });

    group.throughput(Throughput::Elements(1000));
}

// ============================================================
// 崩溃恢复基准测试
// ============================================================

fn bench_recovery(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("recovery");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    group.bench_function("recover_1k_records", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let dir_path = temp_dir.path().to_path_buf();

            // 第一阶段：写入部分数据并创建检查点
            {
                let wal = create_wal(&rt, &dir_path, SyncMode::None);
                let data = vec![0u8; 1024];
                for _ in 0..500 {
                    rt.block_on(wal.write(&data)).unwrap();
                }
                rt.block_on(wal.checkpoint()).unwrap();
                rt.block_on(wal.close()).unwrap();
            }

            // 第二阶段：写入更多数据（不关闭，模拟崩溃）
            {
                let wal = create_wal(&rt, &dir_path, SyncMode::None);
                let data = vec![0u8; 1024];
                for _ in 0..500 {
                    rt.block_on(wal.write(&data)).unwrap();
                }
                // 模拟崩溃：不调用 close()
            }

            // 第三阶段：恢复
            {
                let wal = create_wal(&rt, &dir_path, SyncMode::None);
                let result = rt.block_on(wal.recover(RecoveryMode::FullScan)).unwrap();

                // 验证恢复的记录数
                let recovered = result.records_recovered;
                assert!(
                    recovered >= 500,
                    "Expected at least 500 recovered, got {}",
                    recovered
                );

                // 继续读取验证
                let mut count = recovered as u64;
                loop {
                    match rt.block_on(wal.read()) {
                        Ok(_) => count += 1,
                        Err(easy_wal::Error::Eof) => break,
                        Err(e) => panic!("Read error after recovery: {:?}", e),
                    }
                }
                assert_eq!(count, 1000, "Expected 1000 total records, got {}", count);

                rt.block_on(wal.close()).unwrap();
            }
        });
    });

    group.throughput(Throughput::Elements(1000));
}

// ============================================================
// 并发写入基准测试
// ============================================================

fn bench_concurrent_write(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let handle = rt.handle().clone();

    let mut group = c.benchmark_group("concurrent_write");
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    group.bench_function("4_tasks_1k_records_each", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let handle = handle.clone();
            let wal = Arc::new(create_wal(&rt, temp_dir.path(), SyncMode::None));

            let handles: Vec<_> = (0..4)
                .map(|task_id| {
                    let wal = wal.clone();
                    let handle = handle.clone();
                    thread::spawn(move || {
                        let data = format!("task_{}_data", task_id);
                        let _ = handle.block_on(async {
                            for _ in 0..1000 {
                                wal.write(data.as_bytes()).await.unwrap();
                            }
                        });
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }

            rt.block_on(wal.close()).unwrap();
        });
    });

    group.throughput(Throughput::Elements(4000));
}

// ============================================================
// QPS 综合基准测试
// ============================================================

fn bench_qps_overall(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("qps_overall");
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("qps_100k_target", |b| {
        b.iter(|| {
            let temp_dir = tempdir().unwrap();
            let wal = create_wal(&rt, temp_dir.path(), SyncMode::None);

            let data = vec![0u8; 100]; // 100 字节
            let start = std::time::Instant::now();
            let mut count = 0u64;

            // 运行 5 秒或达到 10 万条
            while start.elapsed().as_secs() < 5 && count < 100000 {
                rt.block_on(wal.write(&data)).unwrap();
                count += 1;
                black_box(count);
            }

            let elapsed = start.elapsed();
            let qps = count as f64 / elapsed.as_secs_f64();

            println!("QPS: {:.2} ({} records in {:?})", qps, count, elapsed);

            rt.block_on(wal.close()).unwrap();
        });
    });
}

criterion_group!(
    benches,
    bench_write_throughput,
    bench_read_throughput,
    bench_batch_write,
    bench_batch_read,
    bench_recovery,
    bench_concurrent_write,
    bench_qps_overall
);
criterion_main!(benches);
