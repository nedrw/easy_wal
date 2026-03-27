# TODO

## 待完成

- [ ] 性能基准测试（目标：10万+ QPS）
  - 创建 `benches/bench.rs`
  - 启用 Cargo.toml bench 配置

- [ ] 补充集成测试
  - WalManager 完整生命周期测试
  - RecoveryManager 场景测试
  - 协调器协作测试

## 已完成

- [x] 删除重复的 storage/wal_manager.rs
- [x] 四层架构重构
- [x] 段轮转机制
- [x] 读取功能
- [x] 恢复机制
- [x] Recovery O(n²) → O(n) 优化
- [x] 预读缓冲区可配置化
- [x] CRC32 数据完整性验证
  - 段文件头 (Magic + Version + Created): 16 bytes
  - 记录格式 [4B Magic][4B Length][4B CRC32][Data...]: 12B + 数据
  - 每条记录带 Magic (0x57414C01)，可快速定位有效记录
  - verify_record 验证 Magic + Length + CRC32
  - find_next_magic 用于损坏时快速跳到下一条记录
  - LogWriter/LogReader/RecoveryManager 全部对齐新格式
- [x] SyncStrategy 架构决策
  - 决策：保持分离，`LogWriter` 保持简单，策略在 API 层管理
  - `SyncStrategy` 已完整实现（None/FsyncOnWrite/Periodic/Batch）
  - 当前 `LogWriter` 使用 `sync_on_write: bool` 已满足需求
  - 复杂策略保留给上层应用自行使用