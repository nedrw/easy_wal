# TODO

## 待完成

- [ ] 性能基准测试（目标：10万+ QPS）
  - 创建 `benches/bench.rs`
  - 添加写入/读取/恢复/并发基准测试
  - 启用 criterion 依赖（取消 Cargo.toml 注释）

- [ ] Recovery 滑动窗口验证（8字节对齐）
  - 实现 8 字节对齐前进
  - 添加 Magic Number 标记优化扫描性能
  - 修复 `src/wal/recovery.rs` 第 485 行逐字节前进问题

- [ ] SyncStrategy 集成到 LogWriter
  - 将 `sync_on_write: bool` 改为 `sync_mode: SyncMode`
  - 集成 SyncStrategy 统计功能
  - 暴露同步指标

- [ ] 预读缓冲区可配置化
  - 在 WalConfig 添加 `read_ahead_size` 配置项
  - 通过 ReadCoordinator 传递配置
  - 修复 `src/wal/coordinators.rs` 第 108 行硬编码问题

- [ ] 补充集成测试
  - 添加 WalManager 完整生命周期测试
  - 添加 RecoveryManager 场景测试（正常/部分损坏/完全损坏）
  - 添加协调器协作测试

## 已完成

- ✅ 删除重复的 storage/wal_manager.rs
- ✅ 四层架构重构（存储层/组件层/协调层/API层）
- ✅ 段轮转机制实现
- ✅ 读取功能实现
- ✅ 恢复机制基础实现