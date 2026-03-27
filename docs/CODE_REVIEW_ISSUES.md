# 代码 Review 问题记录

**Review 日期**: 2025-01-09  
**更新日期**: 2025-01-15

---

## 待修复问题

### 1. 缺少性能基准测试

**位置**: `Cargo.toml#L19-21`

**问题**: criterion 依赖已添加但 bench 被注释，缺少 `benches/` 目录，目标 10万+ QPS 无验证。

**建议修复**:
1. 取消 criterion bench 注释
2. 创建 `benches/bench.rs` 包含写入/读取/恢复/并发基准测试

---

### 2. SyncStrategy 架构（架构讨论中）

**位置**: `src/wal/sync_strategy.rs`, `src/storage/log_writer.rs`

**当前状态**: `SyncStrategy` 已在 `src/wal/` 但 `LogWriter` 使用简单 `bool sync_on_write`

**路径 A - 保持分离（推荐）**: `SyncStrategy` 在 API 层使用，`LogWriter` 保持简单
**路径 B - 集成**: 将 `sync_on_write: bool` 改为 `sync_mode: SyncMode`

---

### 3. 测试覆盖不足

**现有测试**: Storage trait、FileStorage 并发、SegmentManager 轮转、LogWriter 写入、MemoryStorage

**缺少的高级测试**:
- WalManager 完整生命周期测试（创建→写入→崩溃→恢复）
- RecoveryManager 场景测试（正常/部分损坏/完全损坏）
- 协调器协作测试
- 检查点创建/加载/删除流程测试
- 段轮转期间并发读写测试

---

## 问题优先级

| 优先级 | 问题 | 修复复杂度 |
|--------|------|------------|
| P1 | 性能基准测试 | 中 |
| P2 | SyncStrategy 集成决策 | 低（决策） |
| P3 | 测试覆盖不足 | 中 |

---

*Review 更新 - 2025-01-15*