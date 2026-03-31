# Easy WAL

基于 Kafka Log 模式的极简 WAL 库。

## 设计理念

**极简架构**：借鉴 Kafka Log 的设计思想，避免过度分层，状态集中管理。

**核心原则**：
- 单一入口：WAL 对象作为唯一对外接口
- 状态一致：读写共享同一个段对象
- 职责清晰：组件层只负责段内操作，协调层负责段管理决策

## 核心特性

- **简单直接**：2 层架构（WAL → LogSegment），无独立协调层
- **状态一致**：读写共享状态，消除同步问题
- **高性能**：内置预读缓冲区和批量写入优化
- **可靠性强**：数据完整性校验（CRC32）和崩溃恢复

## 参考设计

主要参考 Kafka Log 的段管理设计，同时借鉴 etcd/raft WAL、RocksDB WAL 和 SQLite WAL 的简洁架构。

详细设计见 [`docs/architecture-design.md`](docs/architecture-design.md)。

## 开发状态

项目正在重新设计中，基于主流 WAL 库的最佳实践从零开始构建。