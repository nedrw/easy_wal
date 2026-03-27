# 代码 Review 问题记录

**Review 日期**: 2025-01-09  
**更新日期**: 2025-01-14  
**文档基准**: docs/PROGRESS.md, docs/TODO.md

---

## 已修复

### ✅ 删除重复的 WalManager

**问题**: `src/storage/wal_manager.rs` 违反四层架构，与 `src/wal/wal_manager.rs` 功能重复。

**修复**: 已删除 `src/storage/wal_manager.rs`。

**原因**: 
- WalManager 应只存在于 API 层（`src/wal/`）
- 避免代码重复和维护混乱

---

## 待修复问题详细分析

### 1. Recovery 扫描性能问题 ⚠️ 未修复

**位置**: `src/wal/recovery.rs#L473-485`

```src/wal/recovery.rs#L480-485
Ok(false) => {
    // 记录损坏，跳过到下一个可能的记录位置
    // 注意：逐字节前进是 O(n²) 的，但在实际场景中损坏通常是局部的
    // 如果需要更高性能，可以考虑添加 magic number 标记记录边界
    corrupted_skipped += 1;
    offset += 1; // 逐字节前进寻找下一个可能的长度前缀
}
```

**问题分析**:
- 当记录损坏时，代码逐字节 `offset += 1` 前进，最坏情况 O(n²)
- 大文件（GB级别）恢复可能需要数小时

**建议修复**:
1. 实现 8 字节对齐前进（记录长度前缀是 8 字节对齐）
2. 添加 Magic Number 标记记录边界（如 `0xDEADBEEF`）
3. 跳过整个校验失败区域而非逐字节

---

### 2. 缺少性能基准测试 ⚠️ 未修复

**位置**: `Cargo.toml#L19-21`

```Cargo.toml#L19-21
# [[bench]]
# name = "bench"
# harness = false
```

**问题分析**:
- criterion 依赖已添加但被注释
- 缺少 `benches/` 目录
- 目标 10万+ QPS 无验证

**建议修复**:
1. 取消 criterion bench 注释
2. 创建 `benches/bench.rs` 包含:
   - 写入吞吐量基准测试
   - 读取吞吐量基准测试
   - 恢复性能基准测试
   - 并发写入基准测试

---

### 3. SyncStrategy 位置已修复 ⚠️ 未集成（架构讨论中）

**位置**: `src/wal/sync_strategy.rs` ✅ 已移动到正确位置

**修复内容**:
- `SyncStrategy` 从 `src/storage/` 移动到 `src/wal/`
- 同步策略属于协调层/API层，不属于底层存储层
- 符合四层架构设计原则

**当前 LogWriter 配置** (`src/storage/log_writer.rs#L27-33`):
```src/storage/log_writer.rs#L27-33
pub struct LogWriterConfig {
    pub segment_config: SegmentConfig,
    pub sync_on_write: bool,  // 仅简单的布尔控制
    pub buffer_size: usize,
}
```

**SyncStrategy 已实现的模式** (`src/storage/sync_strategy.rs`):
- `SyncMode::None` - 不同步，依赖 OS 缓冲区
- `SyncMode::FsyncOnWrite` - 每次写入后同步
- `SyncMode::SyncOnBatch` - 批量写入后同步
- `SyncMode::Periodic { interval_ms }` - 定期同步
- `SyncMode::FdataSync` - 使用 fdatasync()

---

## 流行 WAL 库的同步策略对比

### RocksDB (C++)

**两种模式**:
- `sync = false`（默认）: WAL 写入不立即同步到磁盘，依赖 OS 页面缓存
- `sync = true`: WAL 写入后立即 fsync

**Group Commit 优化**:
- 多线程并发写入时，将符合条件的待处理写入聚合成一批
- 用一次 fsync 完成一批写入
- 最大批量 1MB，不会主动延迟写入来增加批量大小

**I/O 优化**:
- `recycle_log_file_num = true` 复用 WAL 文件，避免文件创建时的元数据 I/O
- 小写入（40 bytes）可能产生 8KB 写入放大（约 200x）

### Badger (Go - Dgraph)

- 支持 `SyncWAL()` 手动触发同步
- 支持 `TruncateWAL()` 截断旧日志
- 默认异步写入，定期刷新

### 总结对比

| 策略 | 原理 | 性能 | 安全性 | 代表库 |
|------|------|------|--------|--------|
| 每次同步 | 写入后立即 fsync | 低 | 高 | RocksDB (sync=true) |
| 批量同步 | 积累 N 次写入或超时后同步 | 中 | 中 | 简单实现 |
| Group Commit | 多线程写入聚合为一批后同步 | 高 | 取决于批量大小 | RocksDB |
| OS 缓存 | 依赖操作系统刷新 | 最高 | 低（崩溃丢数据） | RocksDB (sync=false) |

---

## 架构建议

**问题根源分析**:
当前 `SyncStrategy` 设计是正确的，与 RocksDB/Badger 一致。问题在于：
1. `LogWriter`（组件层）使用简单的 `bool sync_on_write`
2. `SyncStrategy`（更高级的抽象）未被使用

**两种修复路径**:

**路径 A - 保持分离（推荐）**:
- `SyncStrategy` 在协调层/API层使用
- `LogWriter` 保持简单，不直接依赖 `SyncStrategy`
- 优点：组件层保持简单，职责分离
- 符合四层架构设计原则

**路径 B - 集成到 LogWriter**:
- 将 `LogWriterConfig.sync_on_write: bool` 改为 `sync_mode: SyncMode`
- `LogWriter` 内部集成 `SyncStrategy`
- 优点：单一配置点
- 缺点：增加组件层复杂度

**建议**: 采用路径 A，在 `WalManager`（API层）集成 `SyncStrategy`，而不是强迫组件层感知高级同步概念。

---

### 4. 预读缓冲区硬编码 ⚠️ 未修复

**位置**: `src/wal/coordinators.rs#L108`

```src/wal/coordinators.rs#L103-108
impl ReadCoordinator {
    pub fn new(reader: Arc<RwLock<LogReader>>) -> Self {
        Self {
            reader,
            read_ahead_buffer: Arc::new(RwLock::new(ReadAheadBuffer::new())),
            read_ahead_size: 64 * 1024, // 硬编码 64KB
        }
    }
```

**问题分析**:
- `read_ahead_size` 硬编码为 64KB，无法根据工作负载调整
- 大顺序读取场景可能需要更大缓冲区
- 小记录高并发场景可能需要更小缓冲区避免内存浪费

**建议修复**:
1. 在 `WalConfig` 添加 `read_ahead_size: usize` 配置项
2. 通过 `ReadCoordinator::new()` 或 `with_read_ahead()` 传递

---

### 5. 测试覆盖不足 ⚠️ 部分改进

**现有测试** (`tests/storage_integration.rs`):
- ✅ Storage trait 测试
- ✅ FileStorage 并发测试
- ✅ SegmentManager 轮转测试
- ✅ LogWriter 写入测试
- ✅ MemoryStorage 测试

**缺少的高级测试**:
- ❌ WalManager 完整生命周期测试（创建→写入→崩溃→恢复）
- ❌ RecoveryManager 场景测试（正常/部分损坏/完全损坏）
- ❌ 协调器协作测试（WriteCoordinator + ReadCoordinator 并发）
- ❌ 检查点创建/加载/删除流程测试
- ❌ 段轮转期间并发读写测试

**建议**:
添加 `tests/wal_integration.rs` 和 `tests/recovery_scenarios.rs`

---

## 问题优先级矩阵

| 优先级 | 问题 | 影响 | 修复复杂度 |
|--------|------|------|------------|
| P1 | Recovery O(n²) 扫描 | 大文件恢复极慢 | 中 |
| P2 | SyncStrategy 架构 | 设计讨论中 | 低（决策） |
| P3 | 预读缓冲区硬编码 | 无法调优不同场景 | 低 |
| P4 | 性能基准测试缺失 | 无法验证性能目标 | 中 |
| P5 | 测试覆盖不足 | 可靠性风险 | 中 |

---

## 统计

| 状态 | 数量 |
|------|------|
| 已修复 | 2 |
| 待修复 | 4（包含 SyncStrategy 集成决策） |
| 修复中 | 0 |

### 已修复问题

1. ✅ 删除重复的 WalManager (`src/storage/wal_manager.rs`)
2. ✅ SyncStrategy 位置修正 (从 `src/storage/` 到 `src/wal/`)

---

*Review 完成 - 2025-01-14*