# 代码 Review 问题记录

**Review 日期**: 2025-01-09  
**更新日期**: 2026-03-28

---

## 已完成的改进

### 配置热更新和监控增强 ✅

**实现日期**: 2025-01-16

**问题背景**:
1. **设计简化** - `WalConfig` 和 `WriteCoordinator` 中的 `SyncContext` 都持有 `SyncMode`，存在重复存储
2. **配置热更新** - 需要支持运行时修改同步策略，以适应不同负载场景
3. **监控需求** - 需要暴露更多配置信息用于监控和性能分析

**解决方案**:

1. **配置与状态分离**:
   - `WalConfig` 保存初始配置（配置源头）
   - `SyncContext` 保存运行时状态（运行时状态）
   - 通过 `WalManager` 暴露查询接口，清晰分离配置和状态

2. **配置热更新支持**:
   - `SyncContext::set_mode()` - 运行时切换同步模式
   - `WriteCoordinator::set_sync_mode()` - 协调器级别的设置接口
   - `WalManager::set_sync_mode()` - API 层暴露的配置热更新接口

3. **监控接口完善**:
   - `WalManager::sync_mode()` - 查询当前运行的同步模式
   - `WalManager::sync_stats()` - 获取同步统计信息（同步次数、总耗时、平均延迟）
   - `WriteCoordinator::sync_stats()` - 底层统计信息

**新增 API**:

```rust
impl WalManager {
    /// 获取当前同步模式（用于监控）
    pub async fn sync_mode(&self) -> SyncMode;
    
    /// 设置同步模式（配置热更新）
    pub async fn set_sync_mode(&self, mode: SyncMode);
    
    /// 获取同步统计信息（用于监控）
    pub async fn sync_stats(&self) -> SyncStats;
}

impl WriteCoordinator {
    /// 设置同步模式（运行时修改）
    pub async fn set_sync_mode(&self, mode: SyncMode);
}

impl SyncContext {
    /// 设置同步模式（运行时切换）
    pub fn set_mode(&mut self, mode: SyncMode);
}
```

**设计要点**:
- 切换模式时自动重置内部状态（批量计数器、定时器）
- 保留历史统计信息，不影响监控连续性
- 线程安全：使用 `RwLock` 保护运行时状态
- API 清晰：配置查询、配置更新、监控信息分离

**使用场景**:
```rust
// 场景1: 根据负载动态调整同步策略
if is_high_load() {
    wal.set_sync_mode(SyncMode::Batch { batch_size: 100 }).await;
} else {
    wal.set_sync_mode(SyncMode::FsyncOnWrite).await;
}

// 场景2: 监控同步性能
let stats = wal.sync_stats().await;
println!("平均同步延迟: {}ms", stats.avg_sync_latency());

// 场景3: 查询当前配置
let mode = wal.sync_mode().await;
```

**教学价值**:
- 展示配置与运行时状态的分离设计
- 演示如何实现配置热更新
- 提供监控接口设计的最佳实践
- 说明线程安全的考虑

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

**位置**: `src/wal/sync_strategy.rs`, `src/wal/coordinators.rs`, `src/wal/wal_manager.rs`

**状态**: 已完全集成并优化（2026-03-28 架构重构）

**实现内容**:
- ✅ **架构优化**: `LogWriter` 移除 `SyncMode`，只负责纯写入操作
- ✅ **职责分离**: `WriteCoordinator` 唯一持有 `SyncContext`，负责同步决策
- 支持四种同步模式：None, FsyncOnWrite, Periodic(interval_ms), Batch(batch_size)
- `WalConfig` 和 `WalBuilder` 保留 `with_sync_mode()` API
- `LogWriterConfig` 移除 `with_sync_mode()` 和 `with_sync_on_write()`（不再相关）

**分层架构**:
```
L4 API层 (WalManager) → L3 协调层 (WriteCoordinator) → L2 存储层 (LogWriter) → L1 存储引擎
                              ↓
                         SyncContext (决策何时 sync)
                              ↓
                         调用 LogWriter.sync() (执行)
```

**教学价值**:
- 展示策略模式在实际组件中的集成
- 演示清晰的架构分层：决策层 vs 执行层
- 提供同步性能与数据安全的权衡实践
- 展示状态去重和职责分离的设计

---

### 3. 测试覆盖不足

**现有测试**: Storage trait、FileStorage 并发、SegmentManager 轮转、LogWriter 写入、MemoryStorage、SyncStrategy 单元测试

**缺少的高级测试**:
- WalManager 完整生命周期测试（创建→写入→崩溃→恢复）
- RecoveryManager 场景测试（正常/部分损坏/完全损坏）
- 协调器协作测试
- 检查点创建/加载/删除流程测试
- 段轮转期间并发读写测试
- 不同 SyncMode 的性能对比测试
- **新增**: 预读优化测试（ReadAheadBuffer 填充/读取/边界情况）

---

## 问题优先级

| 优先级 | 问题 | 修复复杂度 |
|--------|------|------------|
| P1 | 性能基准测试 | 中 |
| P2 | 测试覆盖不足（含 SyncMode 性能测试） | 中 |

---

### 4. 预读优化修复 ✅ 已完成

**位置**: `src/wal/coordinators.rs` - `ReadCoordinator`

**状态**: 已修复（2026-03-28）

**问题**:
1. `ReadCoordinator::with_read_ahead()` 是空实现，只设置了 `read_ahead_size` 但没有重新初始化 `ReadAheadBuffer`
2. `ReadAheadBuffer::read()` 方法缺少 Magic 验证，可能导致解析错误
3. `seek_to_start()` 方法存在死锁：持有 `read_ahead_buffer` 写锁时调用 `fill_buffer()`

**修复内容**:
- ✅ `with_read_ahead()` 现在正确重新初始化 `ReadAheadBuffer`
- ✅ `ReadAheadBuffer` 移除冗余的 `size` 字段，使用容量管理
- ✅ `read()` 方法添加 Magic 验证，确保格式正确
- ✅ `seek_to_start()` 修复锁嵌套，使用代码块及时释放锁
- ✅ `fill_buffer()` 简化逻辑，只在缓冲区为空时填充

---

*Review 更新 - 2026-03-28*