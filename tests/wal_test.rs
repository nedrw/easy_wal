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
fn test_wal_config_custom() {
    // 测试自定义配置
    let config = Config::new()
        .with_segment_size(1024 * 1024) // 1MB
        .with_persistence_mode(easy_wal::PersistenceMode::Batch);

    assert_eq!(config.segment_size(), 1024 * 1024);
    assert_eq!(config.persistence_mode(), easy_wal::PersistenceMode::Batch);
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
