
**Review 日期**: 2025-01-09  
**最后更新**: 2025-04-15

---

## P1 问题修复状态

| 问题 | 状态 | 修复说明 |
|------|------|----------|
| 性能基准测试 | ✅ 已完成 | 创建 `benches/bench.rs`，实现写入/读取/批量/恢复/并发/QPS测试 |


---

## 待修复问题

### P1 - 缺少性能基准测试 ✅ 已完成

**修复内容**:
- `Cargo.toml`: 取消 bench 注释
- `benches/bench.rs`: 实现完整基准测试
  - write_throughput: 写入吞吐量（1KB sync/nosync, 100B sync）
  - read_throughput: 读取吞吐量（1KB, 1k records）
  - batch_write: 批量写入（batch 10/100）
  - batch_read: 批量读取
  - recovery: 崩溃恢复（1k records）
  - concurrent_write: 并发写入（4 任务 x 1000）
  - qps_overall: QPS 综合测试（目标 10万+）


---

### P2 - write_batch 非原子性 ✅ 已完成

**位置**: `src/storage/log_writer.rs`

**修复内容**:
- `src/storage/mod.rs`: 在 `Storage` trait 中添加 `append_batch` 方法声明，保证原子性
- `src/storage/file_storage.rs`: 实现 `FileStorage::append_batch`，合并所有数据到单个缓冲区后一次性写入
- `src/storage/memory_storage.rs`: 实现 `MemoryStorage::append_batch`
- `src/storage/log_writer.rs`: 重写 `write_batch` 方法，支持跨段批量写入

**修复说明**: 
1. `append_batch` 方法在写入前一次性获取文件末尾偏移量，将所有记录合并为单个缓冲区后通过一次 `write_all` 调用完成写入，保证段内原子性
2. `write_batch` 实现跨段支持：当批量数据超过单段容量时，自动轮转到新段继续写入
3. 单条记录不可拆分跨段，多条记录可在段内批量追加（段内原子）
4. 添加 `test_batch_write_cross_segment` 测试用例验证跨段批量写入


---

### P2 - 测试覆盖不足

**现有测试**: Storage trait、FileStorage 并发、SegmentManager 轮转、LogWriter 写入、MemoryStorage、SyncStrategy 单元测试

**缺少的高级测试**:
- WalManager 完整生命周期测试（创建→写入→崩溃→恢复）
- RecoveryManager 场景测试（正常/部分损坏/完全损坏）
- 协调器协作测试
- 检查点创建/加载/删除流程测试
- 不同 SyncMode 的性能对比测试

---

### P3 - 段轮转竞态条件

**位置**: `src/storage/segment_manager.rs`

**问题**: `update_active_size` 和 `create_segment` 之间没有原子性保证，多线程并发写入时可能导致段大小计算错误。

**建议修复**: 添加 `Mutex` 保护 `active_size`。

---

### P3 - LogReader::read_next IO 效率问题

**位置**: `src/storage/log_reader.rs#L136-175`

**问题**: 当前实现分 4 次独立 IO 读取（Magic、Length、CRC32、Data），每次 read 都是独立的系统调用，如果记录被截断可能读到无效数据。

```rust
// 当前实现
let magic_bytes = storage.read(offset, 4).await?;      // IO 1
let length_bytes = storage.read(offset + 4, 4).await?;  // IO 2
let crc_bytes = storage.read(crc_offset, 4).await?;    // IO 3
let data = storage.read(data_offset, length).await?;    // IO 4
```

**建议修复**: 预读整个记录头（12 bytes），在内存中解析。

---

### P3 - 缺少 sync 完成回调

**位置**: `src/wal/coordinators.rs`

**问题**: `WriteCoordinator::do_sync` 执行 fsync 但没有回调钩子，外部无法感知同步完成。

**建议修复**: 添加 `on_sync_complete` 回调或事件。

---

### P3 - ReadAheadBuffer 边界情况

**位置**: `src/wal/coordinators.rs#L258-294`

**问题**: 当 buffer 中残留不完整记录时（只能容纳部分记录），`has_data()` 返回 true 但 `read()` 返回 None。导致 `fill_buffer` 认为有数据不重新填充，提前返回 EOF。

**复现**: 64KB buffer / 1036 bytes per record ≈ 63 条记录，第 64 条不完整时触发。

**建议修复**: 当 `read()` 返回 None 时清空 buffer，强制重新填充。

---

### P4 - checksum.rs 重复注释

**位置**: `src/storage/checksum.rs#L9-12`

**问题**: 文档注释重复。

```rust
/// CRC32 校验和计算器
///
/// 使用 CRC32-IEEE 多项式 (0xEDB88320)
/// 这是最广泛使用的 CRC32 标准，与 Ethernet, ZIP 等兼容。
/// CRC32 校验和计算器  <-- 重复
///
/// 使用 CRC32-IEEE 多项式 (0xEDB88320)  <-- 重复
```

---

## 问题优先级汇总

| 优先级 | 问题 | 修复复杂度 |
|--------|------|------------|
| P1 | 性能基准测试 | 中 |
| P2 | write_batch 非原子性 | 低 |
| P2 | 测试覆盖不足 | 中 |
| P3 | 段轮转竞态条件 | 中 |
| P3 | LogReader::read_next IO 效率 | 低 |
| P3 | 缺少 sync 回调 | 低 |
| P3 | ReadAheadBuffer 边界 | 低 |
| P4 | checksum.rs 重复注释 | 低 |

---

## 架构改进建议（未来考虑）

### 1. 策略模式外置

**当前问题**: `SyncStrategy`（SyncContext）嵌套在 `WriteCoordinator` 里，限制了 WAL 的通用性。

**改进建议**: 将 `SyncStrategy` 作为 `WalManager` 的字段，支持运行时注入不同策略。

```rust
pub struct WalManager {
    sync_strategy: Box<dyn SyncStrategy>,  // 可替换
    // ...
}
```

### 2. 共享段管理

**当前问题**: `LogWriter` 和 `LogReader` 各自持有 `SegmentManager`，可能导致状态不一致。

**改进建议**: 引入 `Arc<SharedSegmentManager>` 让读写协调器共享段状态。

```rust
pub struct SharedSegmentManager {
    inner: RwLock<SegmentManager>,
}

pub struct LogWriter {
    shared: Arc<SharedSegmentManager>,
}
```

### 3. 异步迭代器接口

**当前问题**: `LogReader::read_batch` 使用回调风格，不够直观。

**改进建议**: 实现 `AsyncIterator` trait，提供更现代的读取接口。

```rust
impl AsyncIterator for LogReader {
    type Item = Result<Vec<u8>>;
    
    async fn next(&mut self) -> Option<Self::Item> {
        self.read_next().await.ok()
    }
}
```

---

## 多读多写（暂不考虑）

当前实现为**单写多读**架构。多写多读涉及：
- 写冲突处理（锁优化或乐观并发）
- 复制和高可用
- 分布式一致性

**决定**: 留到单写多读完全实现后再考虑。

---
*Review 更新 - 2025-03-28*
