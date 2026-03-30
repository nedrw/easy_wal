# Easy WAL 已知问题

## 问题 #1: ReadCoordinator 预读缓冲区位置跟踪问题

**状态**: ⚠️ 未修复  
**严重程度**: 中  
**影响版本**: v0.1.0  
**发现日期**: 2026-03-30

---

### 问题描述

在不关闭 WAL 的情况下连续进行写入和读取操作时，读取操作只能返回部分记录（约 63 条），而不是所有已写入的记录。

**症状**:
- 写入 N 条记录（N > 63）
- 调用 `seek_to_start()` 后调用 `read()`
- 只读取到约 63 条记录后返回 EOF
- 关闭并重新打开 WAL 后，可以正确读取所有记录

---

### 复现步骤

```rust
#[tokio::test]
async fn test_reproduce_issue() {
    let temp_dir = tempdir().unwrap();
    
    // 创建 WAL（不关闭）
    let wal = WalBuilder::new()
        .with_dir(temp_dir.path())
        .with_sync_mode(SyncMode::FsyncOnWrite)
        .build()
        .await
        .unwrap();
    
    // 写入 100 条记录
    let data = vec![0u8; 1024]; // 1KB
    for _ in 0..100 {
        wal.write(&data).await.unwrap();
    }
    
    // 跳到开头读取
    wal.seek_to_start().await;
    
    // 读取验证
    let mut read_count = 0u64;
    loop {
        match wal.read().await {
            Ok(_) => read_count += 1,
            Err(easy_wal::Error::Eof) => break,
            Err(e) => panic!("Read error: {:?}", e),
        }
    }
    
    // ❌ 断言失败：期望 100 条，实际约 63 条
    assert_eq!(read_count, 100);
}
```

---

### 根因分析

#### 架构背景

Easy WAL 的读取架构包含以下组件：

```
WalManager
├── WriteCoordinator (写入协调器)
│   └── SegmentCoordinator (段协调器)
│       └── SegmentManager (段管理器 #1)
└── ReadCoordinator (读取协调器)
    └── LogReader
        └── SegmentManager (段管理器 #2) ← 独立实例
```

**关键问题**: `WriteCoordinator` 和 `LogReader` 各自持有独立的 `SegmentManager` 实例。

#### 问题根源

1. **段元数据不同步**
   - `SegmentCoordinator` 的 `SegmentManager` 在写入时创建段并更新元数据
   - `LogReader` 的 `SegmentManager` 在启动时扫描磁盘，之后不更新
   - 导致两个 `SegmentManager` 的状态不一致

2. **预读缓冲区位置跟踪缺失**
   - `ReadCoordinator` 维护一个 `ReadAheadBuffer`
   - `seek_to_start()` 调用流程：
     ```
     ReadCoordinator::seek_to_start()
     ├── 清空预读缓冲区
     ├── 调用 LogReader::seek_to_start() → 设置 position = (1, 16)
     └── 调用 fill_buffer() → 读取 64KB 数据到缓冲区
                              → LogReader.position 更新为 (1, 65552)
     ```
   - 从缓冲区读取时：
     ```
     ReadCoordinator::read_next()
     ├── read_from_buffer() → 从缓冲区读取数据
     └── ❌ 不更新 LogReader.position
     ```
   - 结果：`wal.position()` 返回的是 `LogReader` 的位置（已更新到 65552），而不是实际读取位置

3. **active_storage 缓存问题**
   - `LogReader::get_storage_for_segment()` 检查 `manager.active_id() == segment_id`
   - 由于两个 `SegmentManager` 实例的 `active_id` 可能不同
   - 导致无法正确获取段文件的存储句柄

#### 已修复的部分问题

本次 Phase 4 修复了段元数据不同步的问题：

- ✅ `SegmentManager::scan_segments()` → 公开方法
- ✅ `LogReader::segments()` → 重新扫描磁盘
- ✅ `SegmentManager::segment_path()` → 直接生成路径
- ✅ `LogReader::get_storage_for_segment()` → 移除 active_id 检查

**但预读缓冲区位置跟踪问题仍未修复。**

---

### 影响范围

#### 受影响的场景

- ❌ 不关闭 WAL 直接连续读写
- ❌ 长时间运行的 WAL 实例（预读缓冲区累积位置偏差）
- ❌ 压力测试和崩溃恢复测试

#### 不受影响的场景

- ✅ 关闭后重新打开 WAL 再读取（集成测试采用此模式）
- ✅ 使用 `read_batch()` 批量读取（可能受影响但未暴露）
- ✅ 单条写入后立即读取（预读缓冲区影响较小）

---

### 修复方案

#### 方案 A: 同步预读缓冲区位置到 LogReader（推荐）

**修改 `ReadCoordinator::read_from_buffer()`**:

```rust
async fn read_from_buffer(&self) -> Option<Vec<u8>> {
    let mut buffer = self.read_ahead_buffer.write().await;
    let data = buffer.read();
    
    // ✅ 同步位置到 LogReader
    if let Some(ref data) = data {
        let mut reader_pos = self.reader.write().await;
        let mut pos = reader_pos.position.write().await;
        pos.offset += data.len() as u64 + format::RECORD_HEADER_SIZE;
    }
    
    data
}
```

**优点**:
- 最小改动
- 保持预读缓冲区优化
- 位置跟踪准确

**缺点**:
- 需要仔细处理并发锁
- 需要处理记录边界（不能简单按字节数累加）

#### 方案 B: 禁用预读缓冲区

**修改 `ReadCoordinator::new()`**:

```rust
pub fn with_read_ahead(mut self, size: usize) -> Self {
    self.read_ahead_size = 0; // 强制禁用
    self.read_ahead_buffer = Arc::new(RwLock::new(ReadAheadBuffer::new(0)));
    self
}
```

**优点**:
- 简单直接
- 彻底避免位置跟踪问题

**缺点**:
- 失去预读优化
- 可能影响读取性能（尤其是批量读取）

#### 方案 C: 重构 ReadCoordinator 位置管理（长期方案）

**设计**:
- `ReadCoordinator` 维护独立的 `read_position` 字段
- 不依赖 `LogReader` 的位置
- `LogReader` 只负责纯粹的 IO 操作

```rust
pub struct ReadCoordinator {
    reader: Arc<RwLock<LogReader>>,
    read_ahead_buffer: Arc<RwLock<ReadAheadBuffer>>,
    read_position: Arc<RwLock<ReadPosition>>, // ✅ 独立维护
    read_ahead_size: usize,
}
```

**优点**:
- 架构清晰
- 位置和 IO 分离
- 易于扩展（如支持多读取点）

**缺点**:
- 重构工作量大
- 需要全面测试

---

### 临时解决方案

在修复之前，建议用户采用以下模式：

```rust
// ✅ 推荐：关闭后重新打开再读取
let wal = WalBuilder::new().build().await.unwrap();
// ... 写入数据 ...
wal.close().await.unwrap();

let wal2 = WalBuilder::new().build().await.unwrap();
wal2.seek_to_start().await;
// ... 读取数据 ...

// ✅ 或：使用 read_batch() 而非逐条 read
let records = wal.read_batch(1000).await.unwrap();
```

---

### 修复优先级

| 优先级 | 任务 | 预计工作量 |
|--------|------|-----------|
| P0 | 方案 A: 同步预读缓冲区位置 | 2-3 天 |
| P1 | 方案 C: 重构位置管理 | 5-7 天 |
| P2 | 方案 B: 禁用预读（降级方案） | 0.5 天 |

---

### 相关测试

- `tests/stress_test.rs::test_long_running_stress` - 复现此问题
- `tests/crash_recovery_test.rs` - 多个测试受此影响
- `tests/wal_integration.rs` - 所有集成测试通过（规避了此问题）

---

### 参考资料

- [RocksDB Read Ahead 设计](https://github.com/facebook/rocksdb/wiki/Read-Ahead)
- [PostgreSQL Buffer Manager](https://www.postgresql.org/docs/current/storage-buffer-manager.html)

---

### 更新日志

- 2026-03-30: 创建文档，记录问题详情
- 2026-03-30: Phase 4 修复了段元数据不同步问题，但预读缓冲区位置跟踪仍待修复