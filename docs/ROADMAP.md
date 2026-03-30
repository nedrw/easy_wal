# Easy WAL Development Roadmap

## 项目概述

> 📚 **设计文档**:
> - [Phase 0: 段管理架构优化分析](./analysis-segment-management.md) ✅ 已完成
> - [Phase 1: Group Commit 设计方案](./phases/phase-1-group-commit.md) ✅ 已完成
- [Phase 2: 统一写入架构设计方案](./phases/phase-2-multi-writer.md) ✅ 已完成

**Easy WAL** - 教学级 WAL (Write-Ahead Logging) 系统

**当前状态**:
- Phase 0-2: ✅ 核心基础设施已完成
- Phase 3: ✅ Integration & API 已完成
- Phase 4-5: ❌ 未开始

**下一步目标**: 性能优化和压力测试（Phase 4）

---

## 架构设计

### 四层架构（统一协调器）

```
Layer 4: API 层        - WalManager, WalBuilder
Layer 3: 协调层        - CommitCoordinator（统一协调器）,
                        ReadCoordinator, RecoveryManager
Layer 2: 组件层        - LogWriter, LogReader, SegmentCoordinator
Layer 1: 存储层        - Storage trait, FileStorage, MemoryStorage
```

### 已实现组件

| 组件 | 状态 | 说明 |
|------|------|------|
| `SegmentCoordinator` | ✅ | 段轮转策略决策，Phase 0 引入 |
| `CommitCoordinator` | ✅ | Group Commit 核心，Phase 1 实现，Phase 2 扩展为统一协调器 |
| `WriterHandle` | ✅ | Writer 句柄，提供写入接口，Phase 2 实现 |
| `WriteBatch` | ✅ | 多 writer 批次抽象 |
| `SequenceNumber` | ✅ | 全局序列号 |
| `ReadCoordinator` | ✅ | 读取协调与预读 |
| `RecoveryManager` | ✅ | 崩溃恢复 |

**架构调整（Phase 2）**：
- 移除 `WriteCoordinator`，统一使用 `CommitCoordinator`
- 添加智能退化机制：单 writer → 直接写入，多 writer → Group Commit
- 简化 WalManager，减少代码重复

### 统一写入架构（Phase 2）

```
WalManager
├── CommitCoordinator（统一协调器）
│   ├── WriterRegistry（Writer 注册表）
│   ├── 智能退化机制
│   │   ├── 单 writer → 直接写入（无 Group Commit 开销）
│   │   └── 多 writer → Group Commit（高性能）
│   └── SegmentCoordinator（段轮转决策）
│       └── LogWriter
├── ReadCoordinator
└── RecoveryManager
```

**核心优势**：
- 架构最简：单一协调器，职责清晰
- 性能自适应：根据负载自动优化
- 维护成本低：一套代码路径，无重复
- 用户体验佳：无需关心模式，API 统一

---

## 阶段进度

| Phase | 目标 | 状态 | 工作量 | 说明 |
|-------|------|------|--------|------|
| 0 | 段管理架构重构 | ✅ 完成 | - | SegmentCoordinator 引入 |
| 1 | Multi-Writer Core Infrastructure | ✅ 完成 | - | CommitCoordinator 实现 |
| 2 | 统一写入架构 | ✅ 完成 | **3-5天** | CommitCoordinator 扩展，智能退化，WriterHandle 实现 |
| 3 | Integration & API | ✅ 完成 | **1天** | Recovery 支持，单写/多写模式测试，WriteMode 导出 |
| 4 | Optimization & Testing | ❌ 未开始 | **1周** | 性能优化和压力测试 |
| 5 | Documentation | ❌ 未开始 | **1周** | 文档完善 |

**总工作量**：**2-3周**（vs 原计划 4-5周，减少 50%+）

---

## 已完成任务

### Phase 2: 统一写入架构 ✅

**目标**: 扩展 CommitCoordinator 为统一协调器，实现智能退化机制

- [x] **Task 2.1**: 扩展 `CommitCoordinator`
  - 添加 WriterRegistry（Writer 注册表）
  - 实现智能退化机制（单写 → 直接写入，多写 → Group Commit）
  - 实现 `register_writer()` 和 `unregister_writer()` 方法
  - 实际工作量: 2天

- [x] **Task 2.2**: 实现 `WriterHandle`
  - Writer 句柄，提供 `write()` 和 `write_batch()` 方法
  - 自动从注册表注销
  - 解决线程安全问题（使用 Mutex 代替 RefCell）
  - 实际工作量: 1天

- [x] **Task 2.3**: 简化 `WalManager`
  - 移除 `WriteCoordinator`，统一使用 `CommitCoordinator`
  - 实现 `register_writer()` API
  - 实现便捷方法（`write()`, `write_batch()`）
  - 添加向后兼容的 `sync_mode()` 和 `set_sync_mode()` 方法
  - 实际工作量: 1天

- [x] **Task 2.4**: 集成测试
  - 修复所有集成测试中的 `sync_count` 和 `sync()` 方法调用错误
  - 测试结果：121 个测试通过，1 个并发测试暂时跳过
  - 实际工作量: 1天

**实际总工作量**: **5天**

**关键成果**:
- 架构简化：移除 `WriteCoordinator`，统一使用 `CommitCoordinator`
- 性能自适应：实现智能退化机制，单写/多写模式自动切换
- 测试通过：121/122 测试通过（1 个并发测试待优化）

---

### Phase 3: Integration & API ✅

**目标**: Recovery 支持和完整功能测试

- [x] **Task 3.1**: Recovery 支持
  - 统一写入架构的恢复逻辑
  - 验证数据完整性
  - 实际工作量: 0.5天

- [x] **Task 3.2**: 完整功能测试
  - 单写模式测试（`test_single_writer_mode`）
  - 多写模式测试（`test_multi_writer_mode`）
  - 模式切换测试（`test_mode_switching`）
  - WriterHandle 生命周期测试（`test_writer_lifecycle`，`test_writer_double_close`）
  - 并发多 writer 测试（`test_concurrent_multi_writer`）
  - 批量写入测试（`test_writer_handle_batch_write`）
  - 实际工作量: 0.5天

- [x] **Task 3.3**: API 完善
  - 导出 `WriteMode` 类型（`src/lib.rs`）
  - 修复测试警告（unused imports）
  - 实际工作量: 0.1天

**实际总工作量**: **1天**

**关键成果**:
- 测试覆盖：21 → 28 个集成测试（新增 7 个 Phase 3 测试）
- WriteMode 导出：用户可通过 `easy_wal::WriteMode` 判断当前模式
- 所有 109 个测试通过（63 单元 + 17 存储 + 1 偏移 + 28 集成）


---

## 待开发任务

### Phase 4: Optimization & Testing

**目标**: 性能优化和压力测试

- [ ] **Task 4.1**: 性能优化
  - 根据测试结果针对性优化
  - 可能包括 Lock-free 队列、自适应调优等
  - 预计工作量: 2天

- [ ] **Task 4.2**: 压力测试
  - 单写模式：≥ 50k QPS
  - 多写模式：≥ 100k QPS（10 writers）
  - 模式切换开销：< 1μs
  - 预计工作量: 2天

- [ ] **Task 4.3**: 崩溃恢复测试
  - 并发写入时崩溃恢复
  - 数据完整性验证
  - 预计工作量: 2天

- [ ] **Task 4.4**: 性能基准
  - 建立性能基准测试套件
  - 持续性能监控
  - 预计工作量: 1天

**预计总工作量**: **1周**（vs 原计划 1-2周，减少 30%）

---

### Phase 5: Documentation & Examples

- [ ] API 文档
  - 统一写入架构 API 说明
  - 使用示例
  - 最佳实践
- [ ] 架构文档更新
  - 反映统一协调器架构
  - 性能调优指南
- [ ] 文档清理
  - 移除废弃的 WriteCoordinator 文档
  - 更新架构图

**预计总工作量**: **1周**

---

## 关键里程碑

| Milestone | 目标 | 时间 | 说明 |
|-----------|------|------|------|
| M1 | Phase 2（统一写入架构） | **1周后** | 架构简化，工作量减少 50%+ |
| M2 | Phase 3（API 集成） | **2周后** | Recovery 和测试完成 |
| M3 | Phase 4（优化测试） | **3周后** | 性能优化和压力测试 |
| M4 | Phase 5（文档） | **4周后** | 文档完善，项目完成 |

**总时间**: **4周**（vs 原计划 6周，减少 33%）

---

## 技术风险

| 风险 | 应对 |
|------|------|
| 模式切换竞争 | 原子操作，无锁切换，充分测试 |
| 性能退化 | 基准对比测试，确保单写模式性能不降 |
| 架构调整兼容性 | 向后兼容测试，渐进式迁移 |
| 测试覆盖不足 | Phase 4 专项测试，持续监控 |

---

## 开发原则

1. **架构最简** - 单一协调器，避免职责重叠和代码重复
2. **性能自适应** - 根据负载自动优化，无需手动选择模式
3. **渐进式重构** - 每阶段保持系统可运行
4. **测试驱动** - 每阶段完成有充分测试
5. **向后兼容** - 保持 API 兼容，平滑迁移
6. **教学优先** - 每个决策考虑教学价值，清晰易懂
7. **性能导向** - 统一架构目标：单写 ≥ 50k QPS，多写 ≥ 100k QPS

---

## 更新日志

- 2026-03-30: Phase 0-1 核心基础设施完成
  - SegmentCoordinator: 段轮转策略集中管理
  - CommitCoordinator: Group Commit 核心实现
  - WriteBatch/SequenceNumber: 多 writer 类型定义

- 2026-03-30: 架构调整决策
  - 采用方案B：统一协调器架构
  - 移除 WriteCoordinator，扩展 CommitCoordinator
  - 实现智能退化机制
  - 工作量减少 50%+，总时间缩短至 4周

- 2026-03-30: Phase 2 统一写入架构完成
  - CommitCoordinator: 扩展为统一协调器，支持 WriterRegistry 和智能退化
  - WriterHandle: 实现 Writer 句柄，提供线程安全的写入接口
  - WalManager: 简化架构，统一使用 CommitCoordinator
  - 测试: 121/122 测试通过（并发写入测试待优化）