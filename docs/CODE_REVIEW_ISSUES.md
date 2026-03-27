# 代码 Review 问题记录

**Review 日期**: 2025-01-09  
**更新日期**: 2025-01-16

---

## 待修复问题

### 1. 缺少性能基准测试

**位置**: `Cargo.toml#L19-21`

**问题**: criterion 依赖已添加但 bench 被注释，缺少 `benches/` 目录，目标 10万+ QPS 无验证。

**建议修复**:
1. 取消 criterion bench 注释
2. 创建 `benches/bench.rs` 包含写入/读取/恢复/并发基准测试

---

### 2. SyncStrategy 未实际集成

**位置**: `src/wal/sync_strategy.rs`, `src/storage/log_writer.rs`, `src/wal/wal_manager.rs`

**当前状态**: `SyncStrategy` 已在 `src/wal/` 完整实现（包含 None/FsyncOnWrite/Periodic/Batch 四种模式），但 `LogWriter` 仍使用简单 `bool sync_on_write`，`SyncStrategy` 未被任何组件使用。

**决策**: 保持现状（路径 A）
- `LogWriter` 保持简单，仅使用 `sync_on_write: bool`
- `SyncStrategy` 保留在 API 层，供上层应用自行使用（如需复杂策略，在调用方管理）

**原因**: 
- 同步策略更适合在应用层控制
- 保持底层组件简单，避免过度设计
- 当前 `sync_on_write` 模式已满足大部分场景

**状态**: 已实现但不集成 ✅

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
| P2 | 测试覆盖不足 | 中 |

---

*Review 更新 - 2025-01-16*