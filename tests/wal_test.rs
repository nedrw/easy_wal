//! WAL 对象测试
//!
//! 测试 Easy WAL 的主要接口和功能

use easy_wal::{Config, Error, Wal};

use tempfile::TempDir;

#[test]
fn test_create_new_wal() {
    // 测试创建新的 WAL 实例
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    // 验证 WAL 已创建
    assert!(wal_path.exists());
    assert_eq!(wal.path(), wal_path);
}

#[test]
fn test_open_existing_wal() {
    // 测试打开已存在的 WAL
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 先创建一个 WAL
    let config = Config::default();
    let wal1 = Wal::create(&wal_path, config.clone()).unwrap();
    drop(wal1);

    // 然后打开它
    let wal2 = Wal::open(&wal_path).unwrap();
    assert_eq!(wal2.path(), wal_path);
}

#[test]
fn test_wal_config_default() {
    // 测试默认配置
    let config = Config::default();

    // 验证默认配置值
    assert!(config.segment_size() > 0);
    assert_eq!(
        config.persistence_mode(),
        easy_wal::PersistenceMode::Immediate
    );
}

#[test]
fn test_wal_config_custom() {
    // 测试自定义配置
    let config = Config::new()
        .with_segment_size(1024 * 1024) // 1MB
        .with_persistence_mode(easy_wal::PersistenceMode::Batch);

    assert_eq!(config.segment_size(), 1024 * 1024);
    assert_eq!(config.persistence_mode(), easy_wal::PersistenceMode::Batch);
}

#[test]
fn test_wal_write_single_record() {
    // 测试写入单条记录
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    let data = b"test data";
    wal.write(data).unwrap();
}

#[test]
fn test_wal_write_multiple_records() {
    // 测试写入多条记录
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    let records = vec![
        b"record 1".to_vec(),
        b"record 2".to_vec(),
        b"record 3".to_vec(),
    ];

    let mut offsets = vec![];
    for record in &records {
        let offset = wal.write(record).unwrap();
        offsets.push(offset);
    }

    // 验证 offset 递增
    for i in 1..offsets.len() {
        assert!(offsets[i] > offsets[i - 1]);
    }
}

#[test]
fn test_wal_read_single_record() {
    // 测试读取单条记录
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    let original_data = b"test data for reading";
    let offset = wal.write(original_data).unwrap();

    // 读取刚才写入的数据
    let read_data = wal.read(offset).unwrap();

    assert_eq!(read_data.as_slice(), original_data);
}

#[test]
fn test_wal_read_multiple_records() {
    // 测试读取多条记录
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    let records = vec![
        b"record 1".to_vec(),
        b"record 2".to_vec(),
        b"record 3".to_vec(),
    ];

    let mut offsets = vec![];
    for record in &records {
        let offset = wal.write(record).unwrap();
        offsets.push(offset);
    }

    // 读取所有记录并验证
    for (i, offset) in offsets.iter().enumerate() {
        let read_data = wal.read(*offset).unwrap();
        assert_eq!(read_data, records[i]);
    }
}

#[test]
fn test_wal_persistence_immediate() {
    // 测试立即持久化模式
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(easy_wal::PersistenceMode::Immediate);

    let wal = Wal::create(&wal_path, config).unwrap();

    let data = b"immediate persistence test";
    let offset = wal.write(data).unwrap();

    // 重新打开 WAL 验证数据持久化
    drop(wal);
    let wal2 = Wal::open(&wal_path).unwrap();
    let read_data = wal2.read(offset).unwrap();
    assert_eq!(read_data.as_slice(), data);
}

#[test]
fn test_wal_persistence_batch() {
    // 测试批量持久化模式
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new().with_persistence_mode(easy_wal::PersistenceMode::Batch);

    let wal = Wal::create(&wal_path, config).unwrap();

    let data = b"batch persistence test";
    let offset = wal.write(data).unwrap();

    // 需要显式 flush 才能保证持久化
    wal.flush().unwrap();

    // 重新打开 WAL 验证数据持久化
    drop(wal);
    let wal2 = Wal::open(&wal_path).unwrap();
    let read_data = wal2.read(offset).unwrap();
    assert_eq!(read_data.as_slice(), data);
}

#[test]
fn test_wal_segment_rotation() {
    // 测试段轮转
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 设置很小的段大小以触发轮转
    let config = Config::new().with_segment_size(100); // 100 bytes

    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入多条记录，应该会触发段轮转
    let record1 = b"first record - this should be in the first segment";
    let offset1 = wal.write(record1).unwrap();

    let record2 = b"second record - this should trigger rotation";
    let offset2 = wal.write(record2).unwrap();

    // 验证两个 offset 之间的差距足够大（说明在不同段中）
    // 或者验证目录中有多个段文件
    let segment_files: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().extension().unwrap() == "log")
        .collect();

    // 应该至少有两个段文件
    assert!(segment_files.len() >= 2);

    // 验证两个记录都能正确读取
    assert_eq!(wal.read(offset1).unwrap().as_slice(), record1);
    assert_eq!(wal.read(offset2).unwrap().as_slice(), record2);
}

#[test]
fn test_wal_read_invalid_offset() {
    // 测试读取无效的 offset
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    // 尝试读取不存在的 offset
    let result = wal.read(999999);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Corruption { .. }));
}

#[test]
fn test_wal_config_validation() {
    // 测试配置验证
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 段大小为 0 应该失败
    let config = Config::new().with_segment_size(0);
    let result = Wal::create(&wal_path, config);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Config { .. }));
}

#[test]
fn test_wal_close() {
    // 测试关闭 WAL
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入数据
    wal.write(b"test data").unwrap();

    // 关闭 WAL
    wal.close().unwrap();

    // 关闭后尝试写入应该失败
    let result = wal.write(b"after close");
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Closed));

    // 关闭后尝试读取也应该失败
    let result = wal.read(0);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Closed));
}

#[test]
fn test_wal_path_tracking() {
    // 测试 WAL 路径跟踪
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    assert_eq!(wal.path(), wal_path);
}

#[test]
fn test_wal_empty_data() {
    // 测试写入空数据
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    let empty_data = b"";
    let offset = wal.write(empty_data).unwrap();

    // 空数据也应该能正常读取
    let read_data = wal.read(offset).unwrap();
    assert_eq!(read_data.as_slice(), empty_data);
}

#[test]
fn test_wal_large_data() {
    // 测试写入大数据
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    // 1MB 数据
    let large_data = vec![0u8; 1024 * 1024];
    let offset = wal.write(&large_data).unwrap();

    // 验证大数据能正确读写
    let read_data = wal.read(offset).unwrap();
    assert_eq!(read_data.len(), large_data.len());
    assert_eq!(read_data, large_data);
}

#[test]
fn test_wal_concurrent_reads() {
    // 测试并发读取（如果支持）
    use std::sync::Arc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::default();
    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入数据
    let data = b"test data for concurrent read";
    let offset = wal.write(data).unwrap();

    let wal = Arc::new(wal);
    let mut handles = vec![];

    // 启动多个线程并发读取
    for _ in 0..10 {
        let wal_clone = Arc::clone(&wal);
        let offset = offset;
        let data = data.to_vec();

        handles.push(thread::spawn(move || {
            let read_data = wal_clone.read(offset).unwrap();
            assert_eq!(read_data.as_slice(), data.as_slice());
        }));
    }

    // 等待所有线程完成
    for handle in handles {
        handle.join().unwrap();
    }
}
