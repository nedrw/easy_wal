use easy_wal::{SyncMode, WalBuilder};
use tempfile::tempdir;

#[tokio::test]
async fn test_write_vs_write_batch_offset() {
    // 创建两个独立的临时目录
    let temp_dir1 = tempdir().unwrap();
    let temp_dir2 = tempdir().unwrap();

    // 第一个 WAL：使用 write
    let wal1 = WalBuilder::new()
        .with_dir(temp_dir1.path())
        .with_sync_mode(SyncMode::FsyncOnWrite)
        .build()
        .await
        .unwrap();

    let pos1 = wal1.write(b"hello").await.unwrap();

    wal1.close().await.unwrap();

    // 第二个 WAL：使用 write_batch
    let wal2 = WalBuilder::new()
        .with_dir(temp_dir2.path())
        .with_sync_mode(SyncMode::FsyncOnWrite)
        .build()
        .await
        .unwrap();

    let positions = wal2.write_batch(&[b"hello"]).await.unwrap();

    wal2.close().await.unwrap();

    // 验证：write 和 write_batch 应该返回相同的数据起始位置
    assert_eq!(
        pos1.offset, positions[0].offset,
        "Offsets should match: both should point to the start of data (after record header)"
    );
    assert_eq!(pos1.length, positions[0].length, "Lengths should match");
}
