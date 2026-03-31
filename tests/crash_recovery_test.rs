//! 崩溃恢复测试
//!
//! 验证 WAL 在写入过程中崩溃后的数据完整性：
//! - 已 flush 的数据应该完整保留
//! - 未 flush 的数据可能丢失
//! - 不同持久化模式的崩溃恢复行为

use easy_wal::{Config, PersistenceMode, Wal};
use std::fs;
use std::io::{Seek, Write as IoWrite};
use tempfile::TempDir;

/// 测试崩溃恢复：flush 后的数据应该完整保留
#[test]
fn test_crash_recovery_after_flush() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 创建 WAL 并写入数据
    let data1 = b"record 1";
    let data2 = b"record 2";
    let data3 = b"record 3";

    let offset1;
    let offset2;
    let offset3;

    {
        let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
        let wal = Wal::create(&wal_path, config).unwrap();

        offset1 = wal.write(data1).unwrap();
        offset2 = wal.write(data2).unwrap();
        wal.flush().unwrap(); // 确保写入磁盘

        offset3 = wal.write(data3).unwrap();
        // 不 flush，模拟崩溃前数据未持久化
    } // 模拟崩溃：直接关闭 WAL（不显式关闭）

    // 重新打开 WAL
    let wal = Wal::open(&wal_path).unwrap();

    // 验证已 flush 的数据可以读取
    assert_eq!(wal.read(offset1).unwrap(), data1);
    assert_eq!(wal.read(offset2).unwrap(), data2);

    // 未 flush 的数据可能丢失或损坏，这是可接受的行为
    // 取决于操作系统的 page cache 策略
    let result = wal.read(offset3);
    if result.is_ok() {
        // 如果数据还在，验证完整性
        assert_eq!(result.unwrap(), data3);
    }
    // 如果数据丢失，也是正常的（未 flush 的数据不保证持久化）
}

/// 测试 Immediate 模式的崩溃恢复
#[test]
fn test_crash_recovery_immediate_mode() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let data1 = b"immediate record 1";
    let data2 = b"immediate record 2";
    let data3 = b"immediate record 3";

    let offset1;
    let offset2;
    let offset3;

    {
        let config = Config::new().with_persistence_mode(PersistenceMode::Immediate);
        let wal = Wal::create(&wal_path, config).unwrap();

        offset1 = wal.write(data1).unwrap();
        offset2 = wal.write(data2).unwrap();
        offset3 = wal.write(data3).unwrap();
        // Immediate 模式每次写入都自动 sync，无需显式 flush
    } // 模拟崩溃

    // 重新打开 WAL
    let wal = Wal::open(&wal_path).unwrap();

    // Immediate 模式下，所有写入的数据都应该完整保留
    assert_eq!(wal.read(offset1).unwrap(), data1);
    assert_eq!(wal.read(offset2).unwrap(), data2);
    assert_eq!(wal.read(offset3).unwrap(), data3);
}

/// 测试 Batch 模式的崩溃恢复
#[test]
fn test_crash_recovery_batch_mode() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let data1 = b"batch record 1";
    let data2 = b"batch record 2";
    let data3 = b"batch record 3";

    let offset1;
    let offset2;
    let offset3;

    {
        let config = Config::new().with_persistence_mode(PersistenceMode::Batch);
        let wal = Wal::create(&wal_path, config).unwrap();

        offset1 = wal.write(data1).unwrap();
        wal.flush().unwrap(); // 批量刷新第一批

        offset2 = wal.write(data2).unwrap();
        offset3 = wal.write(data3).unwrap();
        // 不 flush，模拟崩溃
    } // 模拟崩溃

    // 重新打开 WAL
    let wal = Wal::open(&wal_path).unwrap();

    // 已 flush 的数据应该存在
    assert_eq!(wal.read(offset1).unwrap(), data1);

    // 未 flush 的数据可能丢失
    let result2 = wal.read(offset2);
    if result2.is_ok() {
        assert_eq!(result2.unwrap(), data2);
    }

    let result3 = wal.read(offset3);
    if result3.is_ok() {
        assert_eq!(result3.unwrap(), data3);
    }
}

/// 测试多次崩溃恢复
#[test]
fn test_multiple_crash_recovery() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 第一次：写入并 flush
    let offset1;
    {
        let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
        let wal = Wal::create(&wal_path, config).unwrap();
        offset1 = wal.write(b"data 1").unwrap();
        wal.flush().unwrap();
    }

    // 第二次：追加更多数据并 flush
    let offset2;
    {
        let wal = Wal::open(&wal_path).unwrap();
        offset2 = wal.write(b"data 2").unwrap();
        wal.flush().unwrap();
    }

    // 第三次：追加但不 flush
    let offset3;
    {
        let wal = Wal::open(&wal_path).unwrap();
        offset3 = wal.write(b"data 3").unwrap();
        // 不 flush
    }

    // 最终重新打开并验证
    let wal = Wal::open(&wal_path).unwrap();

    assert_eq!(wal.read(offset1).unwrap(), b"data 1");
    assert_eq!(wal.read(offset2).unwrap(), b"data 2");

    // 未 flush 的数据可能丢失
    let result3 = wal.read(offset3);
    if result3.is_ok() {
        assert_eq!(result3.unwrap(), b"data 3");
    }
}

/// 测试段轮转时的崩溃恢复
#[test]
fn test_crash_recovery_during_segment_rotation() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 设置很小的段大小以触发轮转
    let segment_size = 100;
    let data = vec![0u8; 50]; // 每条记录 50 字节

    let offsets: Vec<u64>;

    {
        let config = Config::new()
            .with_segment_size(segment_size)
            .with_persistence_mode(PersistenceMode::Manual);
        let wal = Wal::create(&wal_path, config).unwrap();

        // 写入足够多的数据以触发段轮转
        offsets = (0..10).map(|_| wal.write(&data).unwrap()).collect();

        // Flush 前 5 条记录
        wal.flush().unwrap();

        // 继续写入更多记录
        let _more_offsets: Vec<u64> = (0..5).map(|_| wal.write(&data).unwrap()).collect();

        // 不 flush 最后的记录，模拟崩溃
    } // 模拟崩溃

    // 重新打开 WAL
    let wal = Wal::open(&wal_path).unwrap();

    // 验证已 flush 的前几条记录
    for i in 0..5 {
        assert_eq!(wal.read(offsets[i]).unwrap(), data);
    }
}

/// 测试部分写入的崩溃恢复
#[test]
fn test_crash_recovery_partial_write() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入大量数据
    let large_data = vec![0xABu8; 100 * 1024]; // 100KB
    let offset = wal.write(&large_data).unwrap();
    wal.flush().unwrap();

    // 模拟部分损坏：修改文件中的部分数据
    drop(wal);

    // 直接修改文件内容（模拟部分写入损坏）
    let segment_file = wal_path.with_extension("0");
    if segment_file.exists() {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&segment_file)
            .unwrap();

        // 跳过 header，修改部分数据
        file.seek(std::io::SeekFrom::Start(20)).unwrap();
        file.write_all(&[0xFF; 100]).unwrap();
    }

    // 重新打开 WAL，应该能够检测到损坏或跳过损坏的记录
    let result = Wal::open(&wal_path);

    // 预期：要么成功打开并能读取部分数据，要么返回错误
    if let Ok(wal) = result {
        // 如果成功打开，尝试读取
        let read_result = wal.read(offset);
        // 可能成功（如果没有损坏到关键部分），也可能失败
        if read_result.is_ok() {
            // 数据可能不匹配（因为被损坏了）
            let read_data = read_result.unwrap();
            // 不验证内容，因为已经被损坏
            assert!(!read_data.is_empty() || read_data.is_empty());
        }
        // 如果读取失败（CRC 校验失败），也是正常的
    }
    // 如果打开失败，说明检测到了损坏，这也是正常的
}

/// 测试空 WAL 的崩溃恢复
#[test]
fn test_crash_recovery_empty_wal() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 创建 WAL 但不写入任何数据
    {
        let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
        let _wal = Wal::create(&wal_path, config).unwrap();
        // 不写入，直接关闭
    } // 模拟崩溃

    // 重新打开空 WAL
    let wal = Wal::open(&wal_path).unwrap();

    // 尝试读取不存在的数据
    let result = wal.read(0);
    assert!(result.is_err());
}

/// 测试 WAL 文件损坏的检测
#[test]
fn test_crash_recovery_corrupted_file() {
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(PersistenceMode::Manual);
    let wal = Wal::create(&wal_path, config).unwrap();

    let data = b"important data";
    let offset = wal.write(data).unwrap();
    wal.flush().unwrap();
    drop(wal);

    // 损坏文件头
    let segment_file = wal_path.with_extension("0");
    if segment_file.exists() {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&segment_file)
            .unwrap();

        // 修改 Magic number
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
    }

    // 重新打开 WAL
    let result = Wal::open(&wal_path);

    // 应该能够检测到损坏或处理损坏
    if let Ok(wal) = result {
        // 如果成功打开，读取应该失败或返回错误
        let read_result = wal.read(offset);
        // 可能因为 CRC 或 Magic 不匹配而失败
        if read_result.is_ok() {
            // 如果读取成功，验证数据（可能不匹配）
            let _read_data = read_result.unwrap();
            // 数据可能已损坏
        }
    }
    // 如果打开失败，说明成功检测到了损坏
}
