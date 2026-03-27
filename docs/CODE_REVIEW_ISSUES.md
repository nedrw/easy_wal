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

### 2. SyncStrategy 集成 ✅ 已完成

**位置**: `src/wal/sync_strategy.rs`, `src/storage/log_writer.rs`, `src/wal/wal_manager.rs`

**状态**: 已完全集成

**实现内容**:
- `LogWriter` 现在使用 `SyncMode` 替代简单的 `bool sync_on_write`
- 支持四种同步模式：None, FsyncOnWrite, Periodic(interval_ms), Batch(batch_size)
- `WalConfig` 和 `WalBuilder` 新增 `with_sync_mode()` API
- 保持向后兼容：`with_sync_on_write(true/false)` 自动映射到 FsyncOnWrite/None
- `LogWriter` 新增 `sync_stats()` 和 `sync_mode()` 方法用于监控

**教学价值**:
- 展示策略模式在实际组件中的集成
- 演示如何在保持 API 兼容的同时升级功能
- 提供同步性能与数据安全的权衡实践

---

### 3. 测试覆盖不足

**现有测试**: Storage trait、FileStorage 并发、SegmentManager 轮转、LogWriter 写入、MemoryStorage、SyncStrategy 单元测试

**缺少的高级测试**:
- WalManager 完整生命周期测试（创建→写入→崩溃→恢复）
- RecoveryManager 场景测试（正常/部分损坏/完全损坏）
- 协调器协作测试
- 检查点创建/加载/删除流程测试
- 段轮转期间并发读写测试
- **新增**: 不同 SyncMode 的性能对比测试

---

## 问题优先级

| 优先级 | 问题 | 修复复杂度 |
|--------|------|------------|
| P1 | 性能基准测试 | 中 |
| P2 | 测试覆盖不足（含 SyncMode 性能测试） | 中 |

---

*Review 更新 - 2025-01-16*