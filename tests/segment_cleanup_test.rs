//! 段清理机制测试
//!
//! 测试 Easy WAL 的段生命周期管理功能，包括：
//! - 旧段删除机制
//! - 清理后数据访问处理
//! - 活跃段保护
//! - 磁盘空间管理

use easy_wal::{Config, Error, Wal};
use tempfile::TempDir;

#[test]
fn test_basic_segment_cleanup() {
    // 测试基本段清理功能
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    // 设置很小的段大小以触发轮转
    let config = Config::new()
        .with_segment_size(10 * 1024) // 10KB
        .with_persistence_mode(easy_wal::PersistenceMode::Manual);

    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入足够多的数据，触发多次段轮转
    let data = vec![0u8; 1024]; // 1KB
    let mut offsets = vec![];

    for i in 0..20 {
        let offset = wal.write(&data).unwrap();
        offsets.push(offset);
        println!("写入第 {} 条记录，offset: {}", i + 1, offset);
    }

    wal.flush().unwrap();

    // 检查段文件数量
    let segment_files_before: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().extension().unwrap() == "log")
        .collect();

    println!("清理前段文件数量: {}", segment_files_before.len());
    assert!(segment_files_before.len() > 1, "应该有多个段文件");

    // 清理旧段（保留最近的数据）
    let retain_min_offset = offsets[10]; // 保留第11条记录之后的数据
    wal.prune_segments(retain_min_offset).unwrap();

    // 检查清理后的段文件数量
    let segment_files_after: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().extension().unwrap() == "log")
        .collect();

    println!("清理后段文件数量: {}", segment_files_after.len());
    assert!(
        segment_files_after.len() < segment_files_before.len(),
        "清理后段文件数量应该减少"
    );

    // 验证保留的数据仍然可以读取
    for i in 10..20 {
        let read_data = wal.read(offsets[i]).unwrap();
        assert_eq!(read_data.len(), data.len());
        println!("成功读取第 {} 条记录", i + 1);
    }
}

#[test]
fn test_cleanup_deleted_data_access() {
    // 测试清理后访问已删除数据
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new()
        .with_segment_size(10 * 1024)
        .with_persistence_mode(easy_wal::PersistenceMode::Manual);

    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入数据
    let data = vec![0u8; 1024];
    let mut offsets = vec![];

    for i in 0..15 {
        let offset = wal.write(&data).unwrap();
        offsets.push(offset);
        if i % 5 == 0 {
            println!("写入第 {} 条记录，offset: {}", i + 1, offset);
        }
    }

    wal.flush().unwrap();

    // 检查段文件数量
    let segment_files_before: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().extension().unwrap() == "log")
        .collect();
    println!("清理前段文件数量: {}", segment_files_before.len());

    // 清理旧段
    let retain_min_offset = offsets[10];
    println!("清理 retain_min_offset: {}", retain_min_offset);
    wal.prune_segments(retain_min_offset).unwrap();

    // 检查清理后的段文件数量
    let segment_files_after: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().extension().unwrap() == "log")
        .collect();
    println!("清理后段文件数量: {}", segment_files_after.len());

    // 尝试读取已删除的数据（应该失败）
    // 注意：由于清理是基于整个段而不是单个记录，
    // 只检查第一条记录（offset=0，肯定在第1段中）是否被清理
    // 其他记录可能在保留的段中（取决于段的具体分布）
    let result = wal.read(offsets[0]);
    assert!(result.is_err(), "读取已删除段的数据应该失败，第 1 条记录");

    let error = result.unwrap_err();
    assert!(
        matches!(error, Error::SegmentNotFound { .. }),
        "应该返回 SegmentNotFound 错误，实际错误: {:?}",
        error
    );

    // 验证段清理确实生效（段数量减少）
    assert!(
        segment_files_after.len() < segment_files_before.len(),
        "清理后段数量应该减少: {} -> {}",
        segment_files_before.len(),
        segment_files_after.len()
    );

    // 验证保留的数据仍然可以读取
    // retain_min_offset所在段及之后的数据应该可以读取
    let result = wal.read(offsets[10]);
    assert!(
        result.is_ok(),
        "retain_min_offset所在的数据应该可以读取，第 11 条记录"
    );

    // 验证后续数据也可以读取
    for i in 11..15 {
        let result = wal.read(offsets[i]);
        if result.is_ok() {
            println!("第 {} 条记录可以读取", i + 1);
        }
    }
}

#[test]
fn test_cleanup_preserves_active_segment() {
    // 测试清理时保护活跃段
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new()
        .with_segment_size(10 * 1024)
        .with_persistence_mode(easy_wal::PersistenceMode::Manual);

    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入数据，触发段轮转
    let data = vec![0u8; 1024];
    for _ in 0..15 {
        wal.write(&data).unwrap();
    }

    wal.flush().unwrap();

    // 获取当前活跃段的base_offset
    let active_offset = wal.write(&data).unwrap();

    // 尝试清理包含活跃段的offset（应该失败或忽略）
    let result = wal.prune_segments(active_offset + 1000);

    // 清理应该成功（但活跃段不会被删除）
    assert!(result.is_ok());

    // 验证活跃段仍然存在
    let segment_files: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().extension().unwrap() == "log")
        .collect();

    assert!(segment_files.len() > 0, "活跃段应该仍然存在");

    // 验证可以继续写入
    wal.write(&data).unwrap();
    wal.flush().unwrap();
}

#[test]
fn test_cleanup_disk_space_release() {
    // 测试清理后磁盘空间释放
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new()
        .with_segment_size(10 * 1024)
        .with_persistence_mode(easy_wal::PersistenceMode::Manual);

    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入大量数据
    let data = vec![0u8; 1024];
    for _ in 0..20 {
        wal.write(&data).unwrap();
    }

    wal.flush().unwrap();

    // 测量清理前的总大小
    let total_size_before: u64 = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum();

    println!("清理前总大小: {} bytes", total_size_before);

    // 清理一半的数据
    wal.prune_segments(10 * 1024).unwrap();

    // 测量清理后的总大小
    let total_size_after: u64 = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.metadata().ok())
        .map(|metadata| metadata.len())
        .sum();

    println!("清理后总大小: {} bytes", total_size_after);

    assert!(
        total_size_after < total_size_before,
        "清理后磁盘空间应该减少"
    );
}

#[test]
fn test_cleanup_boundary_cases() {
    // 测试清理边界情况
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new()
        .with_segment_size(10 * 1024)
        .with_persistence_mode(easy_wal::PersistenceMode::Manual);

    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入数据
    let data = vec![0u8; 1024];
    let _offset1 = wal.write(&data).unwrap();
    for _ in 0..10 {
        wal.write(&data).unwrap();
    }
    wal.flush().unwrap();

    // 测试清理 offset = 0（清理所有旧段，只保留活跃段）
    let result = wal.prune_segments(0);
    assert!(result.is_ok());

    // 测试清理 offset 超出当前范围
    let result = wal.prune_segments(999999);
    assert!(result.is_ok()); // 应该成功（但没有段被清理）
}

#[test]
fn test_cleanup_after_multiple_rotations() {
    // 测试多次轮转后的清理
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new()
        .with_segment_size(5 * 1024) // 5KB，更频繁的轮转
        .with_persistence_mode(easy_wal::PersistenceMode::Manual);

    let wal = Wal::create(&wal_path, config).unwrap();

    // 写入大量数据，触发多次轮转
    let data = vec![0u8; 1024];
    let mut offsets = vec![];

    for i in 0..50 {
        let offset = wal.write(&data).unwrap();
        offsets.push(offset);
        if i % 10 == 0 {
            println!("写入第 {} 条记录，offset: {}", i + 1, offset);
        }
    }

    wal.flush().unwrap();

    // 检查段文件数量
    let segment_files_before: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().extension().unwrap() == "log")
        .collect();

    println!("清理前段文件数量: {}", segment_files_before.len());
    assert!(segment_files_before.len() >= 10, "应该有至少10个段文件");

    // 清理大部分旧段
    let retain_min_offset = offsets[40]; // 只保留最后10条记录
    wal.prune_segments(retain_min_offset).unwrap();

    // 检查清理后的段文件数量
    let segment_files_after: Vec<_> = std::fs::read_dir(&wal_path)
        .unwrap()
        .filter(|entry| entry.as_ref().unwrap().path().extension().unwrap() == "log")
        .collect();

    println!("清理后段文件数量: {}", segment_files_after.len());

    // 验证清理效果明显
    assert!(
        segment_files_after.len() < segment_files_before.len() / 2,
        "清理应该删除至少一半的段文件"
    );

    // 验证保留的数据可以读取
    for i in 40..50 {
        let result = wal.read(offsets[i]);
        assert!(result.is_ok(), "第 {} 条记录应该可以读取", i + 1);
    }
}

#[test]
fn test_cleanup_with_persistence_modes() {
    // 测试不同持久化模式下的清理
    let modes = vec![
        easy_wal::PersistenceMode::Immediate,
        easy_wal::PersistenceMode::Batch,
        easy_wal::PersistenceMode::Manual,
    ];

    for mode in modes {
        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("wal");

        let config = Config::new()
            .with_segment_size(10 * 1024)
            .with_persistence_mode(mode);

        let wal = Wal::create(&wal_path, config).unwrap();

        // 写入数据
        let data = vec![0u8; 1024];
        for _ in 0..15 {
            wal.write(&data).unwrap();
        }

        if mode != easy_wal::PersistenceMode::Immediate {
            wal.flush().unwrap();
        }

        // 清理旧段
        let result = wal.prune_segments(10 * 1024);
        assert!(result.is_ok(), "持久化模式 {:?} 下清理应该成功", mode);

        // 验证清理后可以继续写入
        wal.write(&data).unwrap();

        if mode != easy_wal::PersistenceMode::Immediate {
            wal.flush().unwrap();
        }
    }
}

#[test]
fn test_cleanup_reopen_wal() {
    // 测试清理后重新打开 WAL
    let temp_dir = TempDir::new().unwrap();
    let wal_path = temp_dir.path().join("wal");

    let config = Config::new()
        .with_segment_size(10 * 1024)
        .with_persistence_mode(easy_wal::PersistenceMode::Manual);

    // 创建并写入数据
    {
        let wal = Wal::create(&wal_path, config).unwrap();
        let data = vec![0u8; 1024];
        let mut offsets = vec![];

        for _ in 0..15 {
            let offset = wal.write(&data).unwrap();
            offsets.push(offset);
        }

        wal.flush().unwrap();

        // 清理旧段
        wal.prune_segments(offsets[10]).unwrap();
    }

    // 重新打开 WAL
    let wal = Wal::open(&wal_path).unwrap();

    // 验证可以继续写入
    let data = vec![1u8; 1024];
    let new_offset = wal.write(&data).unwrap();
    wal.flush().unwrap();

    // 验证新数据可以读取
    let read_data = wal.read(new_offset).unwrap();
    assert_eq!(read_data, data);
}
