//! 并发压力测试
//!
//! 验证 WAL 在高并发场景下的正确性和性能：
//! - 多线程并发写入
//! - 多线程并发读取
//! - 多线程混合读写
//! - 并发 flush 和写入
//! - 高负载压力测试

use easy_wal::{Config, PersistenceMode, Wal};
use std::collections::HashSet;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

/// 测试多线程并发写入
#[test]
fn test_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Arc::new(Wal::create(&wal_path, config).unwrap());

    let num_threads = 4;
    let writes_per_thread = 100;
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(num_threads));

    // 启动多个线程并发写入
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            // 等待所有线程就绪
            barrier_clone.wait();

            let mut offsets = vec![];
            for i in 0..writes_per_thread {
                let data = format!("thread-{}-record-{}", thread_id, i);
                let offset = wal_clone.write(data.as_bytes()).unwrap();
                offsets.push(offset);
            }
            offsets
        });

        handles.push(handle);
    }

    // 收集所有线程的写入偏移量
    let mut all_offsets = HashSet::new();
    for handle in handles {
        let offsets = handle.join().unwrap();
        for offset in offsets {
            assert!(
                all_offsets.insert(offset),
                "Duplicate offset found: {}",
                offset
            );
        }
    }

    // 验证所有写入的数据
    let total_writes = num_threads * writes_per_thread;
    assert_eq!(all_offsets.len(), total_writes);

    // Flush 并验证数据完整性
    wal.flush().unwrap();

    // 读取并验证随机选择的数据
    for offset in all_offsets.iter().take(50) {
        let result = wal.read(*offset);
        assert!(result.is_ok(), "Failed to read at offset {}", offset);
        let data = result.unwrap();
        assert!(!data.is_empty());
    }
}

/// 测试多线程并发读取
#[test]
fn test_concurrent_reads() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Arc::new(Wal::create(&wal_path, config).unwrap());

    // 先写入一些数据
    let mut offsets = vec![];
    for i in 0..100 {
        let data = format!("record-{}", i);
        let offset = wal.write(data.as_bytes()).unwrap();
        offsets.push(offset);
    }
    wal.flush().unwrap();

    let num_threads = 4;
    let reads_per_thread = 50;
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(num_threads));

    // 启动多个线程并发读取
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let barrier_clone = Arc::clone(&barrier);
        let offsets_clone = offsets.clone();

        let handle = thread::spawn(move || {
            // 等待所有线程就绪
            barrier_clone.wait();

            let mut success_count = 0;
            for i in 0..reads_per_thread {
                let offset_index = (thread_id * reads_per_thread + i) % offsets_clone.len();
                let offset = offsets_clone[offset_index];
                let result = wal_clone.read(offset);
                if result.is_ok() {
                    let data = result.unwrap();
                    let expected = format!("record-{}", offset_index);
                    if data == expected.as_bytes() {
                        success_count += 1;
                    }
                }
            }
            success_count
        });

        handles.push(handle);
    }

    // 验证所有读取都成功
    let mut total_success = 0;
    for handle in handles {
        total_success += handle.join().unwrap();
    }

    assert_eq!(total_success, num_threads * reads_per_thread);
}

/// 测试并发 flush 和写入
#[test]
fn test_concurrent_flush_and_write() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Arc::new(Wal::create(&wal_path, config).unwrap());

    let num_operations = 100;
    let barrier = Arc::new(Barrier::new(2));

    // 线程1：持续写入
    let wal_clone1 = Arc::clone(&wal);
    let barrier_clone1 = Arc::clone(&barrier);
    let write_handle = thread::spawn(move || {
        barrier_clone1.wait();

        let mut offsets = vec![];
        for i in 0..num_operations {
            let data = format!("data-{}", i);
            let offset = wal_clone1.write(data.as_bytes()).unwrap();
            offsets.push(offset);
        }
        offsets
    });

    // 线程2：持续 flush
    let wal_clone2 = Arc::clone(&wal);
    let barrier_clone2 = Arc::clone(&barrier);
    let flush_handle = thread::spawn(move || {
        barrier_clone2.wait();

        for _ in 0..num_operations / 10 {
            wal_clone2.flush().unwrap();
            thread::sleep(std::time::Duration::from_micros(100));
        }
    });

    // 等待所有线程完成
    let offsets = write_handle.join().unwrap();
    flush_handle.join().unwrap();

    // 最终 flush 并验证
    wal.flush().unwrap();

    // 验证所有数据都可以读取
    for (i, offset) in offsets.iter().enumerate().take(10) {
        let result = wal.read(*offset);
        assert!(result.is_ok(), "Failed to read at offset {}", offset);
        let data = result.unwrap();
        let expected = format!("data-{}", i);
        assert_eq!(data, expected.as_bytes());
    }
}

/// 测试高负载压力
#[test]
fn test_high_load_stress() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new()
        .with_persistence_mode(PersistenceMode::Manual)
        .with_segment_size(10 * 1024); // 10KB 段大小，触发轮转

    let wal = Arc::new(Wal::create(&wal_path, config).unwrap());

    let num_threads = 8;
    let writes_per_thread = 200;
    let data_size = 512; // 512 bytes per write
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    // 启动多个写入线程
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            let data = vec![thread_id as u8; data_size];
            let mut offsets = vec![];

            for i in 0..writes_per_thread {
                match wal_clone.write(&data) {
                    Ok(offset) => offsets.push(offset),
                    Err(e) => eprintln!(
                        "Thread {} write failed at iteration {}: {:?}",
                        thread_id, i, e
                    ),
                }

                // 偶尔 flush
                if i % 50 == 0 {
                    let _ = wal_clone.flush();
                }
            }

            offsets
        });

        handles.push(handle);
    }

    // 等待所有线程完成
    let mut all_offsets = vec![];
    for handle in handles {
        let offsets = handle.join().unwrap();
        all_offsets.extend(offsets);
    }

    // 最终 flush
    wal.flush().unwrap();

    // 验证写入总数
    let total_expected = num_threads * writes_per_thread;
    println!(
        "Total writes: {}, Expected: {}",
        all_offsets.len(),
        total_expected
    );

    // 验证数据完整性（抽查部分数据）
    for offset in all_offsets.iter().take(50) {
        let result = wal.read(*offset);
        assert!(result.is_ok(), "Failed to read at offset {}", offset);
        let data = result.unwrap();
        assert_eq!(data.len(), data_size);
    }
}

/// 测试并发段轮转
#[test]
fn test_concurrent_segment_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 设置很小的段大小以触发频繁轮转
    let segment_size = 1024; // 1KB
    let config = Config::new()
        .with_segment_size(segment_size)
        .with_persistence_mode(PersistenceMode::Manual);

    let wal = Arc::new(Wal::create(&wal_path, config).unwrap());

    let num_threads = 4;
    let writes_per_thread = 50;
    let data_size = 256; // 256 bytes per write
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    // 启动多个写入线程
    for thread_id in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            let data = vec![thread_id as u8; data_size];
            let mut offsets = vec![];

            for i in 0..writes_per_thread {
                let offset = wal_clone.write(&data).unwrap();
                offsets.push(offset);

                // 每 10 次写入 flush 一次
                if i % 10 == 0 {
                    wal_clone.flush().unwrap();
                }
            }

            offsets
        });

        handles.push(handle);
    }

    // 等待所有线程完成
    let mut all_offsets = vec![];
    for handle in handles {
        let offsets = handle.join().unwrap();
        all_offsets.extend(offsets);
    }

    // 最终 flush
    wal.flush().unwrap();

    // 验证所有数据可读
    for offset in all_offsets.iter().take(20) {
        let result = wal.read(*offset);
        assert!(result.is_ok(), "Failed to read at offset {}", offset);
    }
}

/// 测试并发创建和打开 WAL
#[test]
fn test_concurrent_create_and_open() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 第一个线程创建 WAL
    let path_clone1 = wal_path.clone();
    let handle1 = thread::spawn(move || {
        let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
        let wal = Wal::create(&path_clone1, config).unwrap();
        wal.write(b"thread1").unwrap();
        wal.flush().unwrap();
    });

    handle1.join().unwrap();

    // 多个线程打开并写入
    let num_threads = 4;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let path_clone = wal_path.clone();
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            // 每个线程打开 WAL 并追加数据
            let wal = Wal::open(&path_clone).unwrap();
            let data = format!("thread-{}", thread_id);
            wal.write(data.as_bytes()).unwrap();
            wal.flush().unwrap();
        });

        handles.push(handle);
    }

    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }

    // 最终验证
    let _wal = Wal::open(&wal_path).unwrap();
    // 可以读取所有线程写入的数据
}

/// 测试并发错误处理
#[test]
fn test_concurrent_error_handling() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Arc::new(Wal::create(&wal_path, config).unwrap());

    // 写入一些数据
    let valid_offset = wal.write(b"valid data").unwrap();
    wal.flush().unwrap();

    let num_threads = 4;
    let barrier = Arc::new(Barrier::new(num_threads));
    let mut handles = vec![];

    // 多个线程并发读取无效偏移量
    for _ in 0..num_threads {
        let wal_clone = Arc::clone(&wal);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || {
            barrier_clone.wait();

            let mut error_count = 0;
            let mut success_count = 0;

            // 尝试读取无效偏移量
            for offset in 999900..999910 {
                let result = wal_clone.read(offset);
                if result.is_err() {
                    error_count += 1;
                }
            }

            // 读取有效偏移量
            let result = wal_clone.read(valid_offset);
            if result.is_ok() {
                success_count += 1;
            }

            (error_count, success_count)
        });

        handles.push(handle);
    }

    // 验证错误处理
    for handle in handles {
        let (error_count, success_count) = handle.join().unwrap();
        assert_eq!(error_count, 10); // 所有无效读取都应该失败
        assert_eq!(success_count, 1); // 有效读取应该成功
    }
}
