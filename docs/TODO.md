# TODO - 优化事项记录

本文档记录发现的问题和优化方向，已按阶段分类。

---

## Phase 5: 性能优化

### 1. Coordinators 逻辑增强
**问题**: 当前 `WriteCoordinator` 和 `ReadCoordinator` 较薄，未实现批量优化、请求队列等功能
**建议**: 
- WriteCoordinator: 实现批量写入缓冲、写入队列
- ReadCoordinator: 实现预读缓冲、并发读取控制

### 2. Recovery 扫描策略优化
**问题**: FullScan 模式逐字节前进（O(n²)），大文件效率低
**建议**: 实现 magic number 或块对齐优化

---

## 已废弃项

以下项目经分析后判定为无效优化，关闭：

- ~~目录结构优化~~ - `storage/` 命名合理，无需改名
- ~~RecoveryManager 存储依赖~~ - Checkpoint 是元数据，独立存储是正确的设计选择
- ~~Checkpoint 存储格式~~ - 二进制是 WAL 标准做法，JSON/Protobuf 增加复杂度无收益

---

## 未来 Phase 计划

参见 [PROGRESS.md](../PROGRESS.md)

- Phase 5: 性能优化
- Phase 6: 可靠性增强  
- Phase 7: 监控和运维