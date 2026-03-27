# TODO - 优化事项记录

本文档记录发现的问题和优化方向，已按阶段分类。

---

## Phase 5: 性能优化

### ✅ 已完成
- ✅ WriteCoordinator 简化（减少锁竞争）
- ✅ ReadCoordinator 预读缓冲（64KB）
- ✅ Recovery O(n²) 问题文档化

### 🔲 后续计划
- [ ] **性能基准测试**（10万+ QPS 目标）- 适合在 Phase 5 整体调优时完成
- [ ] Recovery 滑动窗口验证（8字节对齐前进）- 参见 PHASE5_IMPLEMENTATION.md
- [ ] WriteCoordinator 批量写入缓冲

### ⚠️ 待解决
**问题**: FullScan 模式逐字节前进（O(n²)），大文件效率低
**建议**: 实现 magic number 或块对齐优化（参见 WAL_FORMAT.md）

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