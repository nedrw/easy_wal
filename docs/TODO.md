# TODO - 优化事项记录

本文档记录待完成事项和未来优化方向。

---

## 待完成

- [ ] **性能基准测试**（10万+ QPS 目标）
- [ ] Recovery 滑动窗口验证（8字节对齐前进）- 参见 WAL_FORMAT.md
- [ ] Magic Number 标记（O(n) 扫描优化）- 参见 WAL_FORMAT.md

---

## 已废弃项

| 项目 | 原因 |
|------|------|
| ~~事务支持~~ | WAL 不应实现事务，事务应由使用方在上层实现 |
| ~~目录结构优化~~ | `storage/` 命名合理，无需改名 |
| ~~RecoveryManager 存储依赖~~ | Checkpoint 是元数据，独立存储是正确的设计选择 |
| ~~Checkpoint 存储格式~~ | 二进制是 WAL 标准做法，JSON/Protobuf 增加复杂度无收益 |

---

## 后续计划

参见 [PROGRESS.md](./PROGRESS.md)

- Phase 7: 监控和运维
- Phase 8: 文档完善
- Phase 9: 压力测试