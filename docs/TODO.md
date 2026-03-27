# TODO - 优化事项记录

本文档记录 Phase 4 重构后发现的优化方向，按优先级排序。

---

## 架构优化

### 1. 目录结构优化
**优先级**: 中
**问题**: `storage/` 目录同时包含 Layer 1 (Storage trait) 和 Layer 2 (LogWriter, LogReader, SegmentManager)
**建议**: 重命名为 `components/` 目录，更清晰反映其作为组件层的定位

### 2. RecoveryManager 存储依赖
**优先级**: 中
**问题**: `RecoveryManager` 直接依赖 `FileStorage`，绕过了 `Storage` trait 抽象
**建议**: 修改为通过 `Storage` trait 操作，便于测试和扩展

### 3. Coordinators 逻辑增强
**优先级**: 低
**问题**: 当前 `WriteCoordinator` 和 `ReadCoordinator` 较薄，未实现批量优化、请求队列等功能
**建议**: 
- WriteCoordinator: 实现批量写入缓冲、写入队列
- ReadCoordinator: 实现预读缓冲、并发读取控制

---

## 待讨论事项

### 4. Checkpoint 存储格式
**问题**: 当前 Checkpoint 使用自定义二进制格式，版本兼容性未考虑
**建议**: 考虑使用 JSON 或 Protobuf，便于版本演进

### 5. Recovery 策略
**问题**: 当前 FullScan 模式逐字节前进找长度前缀，效率低
**建议**: 实现 magic number 或块对齐优化

---

## 未来 Phase 计划

参见 [PROGRESS.md](../PROGRESS.md)

- Phase 5: 性能优化
- Phase 6: 可靠性增强  
- Phase 7: 监控和运维