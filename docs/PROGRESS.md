# WAL 项目进度

## 架构
```
Layer 4: API 层        - WalManager, WalBuilder (src/wal/)
Layer 3: 协调层        - WriteCoordinator, ReadCoordinator, RecoveryManager, SyncStrategy (src/wal/)
Layer 2: 组件层        - LogWriter, LogReader, SegmentManager (src/storage/)
Layer 1: 存储层        - Storage trait, FileStorage, MemoryStorage (src/storage/)
```

## 阶段进度

| Phase | 目标 | 状态 |
|-------|------|------|
| 1 | 存储层重构 | ✅ |
| 2 | 文件管理（段轮转） | ✅ |
| 3 | 读取功能 | ✅ |
| 4 | 恢复机制 | ✅ |
| 5 | 性能优化 | ✅ |
| 6 | 可靠性增强 | ✅ |
| 7 | 错误处理与状态查询 | ✅ |
| 8 | 文档和示例 | ⬜ |
| 9 | 压力测试 | ⬜ |

**当前版本**: v0.7.1

## 更新日志

### v0.7.1 (2025-01-14)
- **架构调整**: `SyncStrategy` 从 `src/storage/` 移动到 `src/wal/`
  - 同步策略属于协调层/API层设计，不属于底层存储层
  - 保持四层架构职责清晰

## 待办事项

参见 [TODO.md](./TODO.md)