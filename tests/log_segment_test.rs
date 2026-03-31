//! LogSegment 测试
//!
//! 测试 Easy WAL 的段管理功能

use easy_wal::{Error, LogSegment};

use tempfile::TempDir;

#[test]
fn test_create_new_segment() {
    // 测试创建新的 LogSegment
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    let segment = LogSegment::create(&segment_path, 0).unwrap();

    // 验证 segment 已创建
    assert!(segment_path.exists());
    assert_eq!(segment.base_offset(), 0);
    assert_eq!(segment.size(), 0);
}

#[test]
fn test_open_existing_segment() {
    // 测试打开已存在的 LogSegment
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    // 先创建一个 segment
    let segment1 = LogSegment::create(&segment_path, 0).unwrap();
    drop(segment1);

    // 然后打开它
    let segment2 = LogSegment::open(&segment_path).unwrap();
    assert_eq!(segment2.base_offset(), 0);
}

#[test]
fn test_segment_base_offset() {
    // 测试不同 base offset 的 segment
    let temp_dir = TempDir::new().unwrap();

    let segment_path1 = temp_dir.path().join("00000000000000001024.log");
    let segment1 = LogSegment::create(&segment_path1, 1024).unwrap();
    assert_eq!(segment1.base_offset(), 1024);

    let segment_path2 = temp_dir.path().join("00000000000000002048.log");
    let segment2 = LogSegment::create(&segment_path2, 2048).unwrap();
    assert_eq!(segment2.base_offset(), 2048);
}

#[test]
fn test_append_single_record() {
    // 测试追加单条记录
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    let segment = LogSegment::create(&segment_path, 0).unwrap();

    let data = b"test data";
    let offset = segment.append(data).unwrap();

    // 验证返回的 offset 是正确的（应该等于 base_offset）
    assert_eq!(offset, 0);

    // 验证 segment 大小增加了
    assert!(segment.size() > 0);
}

#[test]
fn test_append_multiple_records() {
    // 测试追加多条记录
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    let segment = LogSegment::create(&segment_path, 0).unwrap();

    let data1 = b"first record";
    let data2 = b"second record";
    let data3 = b"third record";

    let offset1 = segment.append(data1).unwrap();
    let offset2 = segment.append(data2).unwrap();
    let offset3 = segment.append(data3).unwrap();

    // 验证 offset 递增
    assert!(offset2 > offset1);
    assert!(offset3 > offset2);
}

#[test]
fn test_read_single_record() {
    // 测试读取单条记录
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    let segment = LogSegment::create(&segment_path, 0).unwrap();

    let original_data = b"test data for reading";
    let offset = segment.append(original_data).unwrap();

    // 读取刚才写入的数据
    let read_data = segment.read(offset).unwrap();

    assert_eq!(read_data.as_slice(), original_data);
}

#[test]
fn test_read_multiple_records() {
    // 测试读取多条记录
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    let segment = LogSegment::create(&segment_path, 0).unwrap();

    let records = vec![
        b"record 1".to_vec(),
        b"record 2".to_vec(),
        b"record 3".to_vec(),
    ];

    let mut offsets = vec![];
    for record in &records {
        let offset = segment.append(record).unwrap();
        offsets.push(offset);
    }

    // 读取所有记录并验证
    for (i, offset) in offsets.iter().enumerate() {
        let read_data = segment.read(*offset).unwrap();
        assert_eq!(read_data, records[i]);
    }
}

#[test]
fn test_read_invalid_offset() {
    // 测试读取无效的 offset
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    let segment = LogSegment::create(&segment_path, 0).unwrap();

    // 尝试读取不存在的 offset
    let result = segment.read(999);
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), Error::Corruption { .. }));
}

#[test]
fn test_segment_file_naming() {
    // 测试 segment 文件命名规范
    let temp_dir = TempDir::new().unwrap();

    // base_offset = 0
    let path1 = temp_dir.path().join("00000000000000000000.log");
    let _segment1 = LogSegment::create(&path1, 0).unwrap();
    assert!(path1.exists());

    // base_offset = 1024
    let path2 = temp_dir.path().join("00000000000000001024.log");
    let _segment2 = LogSegment::create(&path2, 1024).unwrap();
    assert!(path2.exists());
}

#[test]
fn test_append_empty_data() {
    // 测试追加空数据
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    let segment = LogSegment::create(&segment_path, 0).unwrap();

    let empty_data = b"";
    let offset = segment.append(empty_data).unwrap();

    // 空数据也应该能正常写入
    let read_data = segment.read(offset).unwrap();
    assert_eq!(read_data.as_slice(), empty_data);
}

#[test]
fn test_data_integrity_with_crc() {
    // 测试数据完整性（CRC 校验）
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    let segment = LogSegment::create(&segment_path, 0).unwrap();

    let original_data = b"data with crc check";
    let offset = segment.append(original_data).unwrap();

    // 正常读取应该成功
    let read_data = segment.read(offset).unwrap();
    assert_eq!(read_data.as_slice(), original_data);

    // 注意：这里我们无法直接模拟 CRC 错误，因为我们没有暴露底层文件操作
    // CRC 错误的测试需要在集成测试中通过直接修改文件内容来验证
}

#[test]
fn test_segment_size_tracking() {
    // 测试 segment 大小跟踪
    let temp_dir = TempDir::new().unwrap();
    let segment_path = temp_dir.path().join("00000000000000000000.log");

    let segment = LogSegment::create(&segment_path, 0).unwrap();

    let initial_size = segment.size();
    assert_eq!(initial_size, 0);

    // 写入数据
    let data = b"test data for size tracking";
    segment.append(data).unwrap();

    let new_size = segment.size();
    assert!(new_size > initial_size);

    // 写入更多数据
    segment.append(b"more data").unwrap();
    let final_size = segment.size();

    assert!(final_size > new_size);
}
