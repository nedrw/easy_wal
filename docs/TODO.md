# TODO

## 待完成

- [ ] 性能基准测试（目标：10万+ QPS）
  - 创建 `benches/bench.rs`
  - 启用 Cargo.toml bench 配置

- [ ] 补充集成测试
  - WalManager 完整生命周期测试
  - RecoveryManager 场景测试
  - 协调器协作测试
  - 不同 SyncMode 的性能对比测试
  - 配置热更新场景测试

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
- [x] SyncStrategy 集成到 LogWriter
  - `LogWriterConfig` 使用 `sync_mode: SyncMode` 替代 `sync_on_write: bool`
  - 支持四种同步模式：None、FsyncOnWrite、Periodic(interval_ms)、Batch(batch_size)
  - `WalConfig` 和 `WalBuilder` 新增 `with_sync_mode()` API
  - 保持向后兼容：`with_sync_on_write(true/false)` 自动映射
  - `LogWriter` 新增 `sync_stats()` 和 `sync_mode()` 方法
- [x] 配置热更新和监控增强 (2025-01-16)
  - 配置与运行时状态分离设计
  - 新增 `WalManager::sync_mode()` 查询当前同步模式
  - 新增 `WalManager::set_sync_mode()` 支持运行时切换同步策略
  - 新增 `WalManager::sync_stats()` 暴露同步统计信息
  - 切换模式时自动重置内部状态，保留历史统计
  - 线程安全设计，使用 RwLock 保护运行时状态