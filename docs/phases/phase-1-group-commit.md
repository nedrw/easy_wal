# Phase 1: Group Commit 设计方案

## 目标

实现 RocksDB-style Group Commit，为 Multi-Writer 提供核心基础设施。

## 核心组件

| 组件 | 职责 | 关键字段 |
|------|------|----------|
| `CommitCoordinator` | 批次收集、合并写入、批量 fsync、结果通知 | pending_batches, commit_loop |
| `WriteBatch` | 单 writer 批次抽象 | batch_id, writer_id, records, sequence |
| `CommitConfig` | Group Commit 配置 | max_batch_size, max_wait_time, min_batches |
| `SequenceNumber` | 全局序列号 | commit_group(高32位) + sequence(低32位) |

## 架构

```
Writer1 ─────┐
Writer2 ─────┼──► WriteBatch (lock-free build)
Writer3 ─────┘            │
                          ▼
         ┌────────────────────────┐
         │   Commit Coordinator    │
         │  [Batch1, Batch2, ...] │
         │          │              │
         │          ▼              │
         │   Group Commit Loop    │
         │   1. Collect           │
         │   2. Merge buffer      │
         │   3. Single fsync     │
         │   4. Notify writers    │
         └────────────────────────┘
                          │
                          ▼
                 SegmentCoordinator
                          │
                          ▼
                      LogWriter
```

## 提交条件

触发提交满足任一条件：
- `max_batch_size` (64KB 默认) 达到
- `max_wait_time_ms` (5ms 默认) 达到
- `max_batch_count` (100 默认) 达到

## 性能对比

| 方面 | Single-Writer | Group Commit |
|------|---------------|--------------|
| 延迟 | I/O bound | 更低 (批次 I/O) |
| 吞吐 | 受 fsync 频率限制 | 批次聚合提升 |
| 锁竞争 | 高 | 低 |

## Recovery

- 启动扫描所有段，定位最后有效记录 (magic + CRC)
- committed_sequence = last_valid_sequence
- 未 fsync 的批次丢失（可接受）

## 向后兼容

- WAL 文件格式不变
- 单写模式：不用 `with_multi_writer()` 时行为不变

## 待实现

- MultiWriterCoordinator
- WriterHandle
- WalBuilder 扩展
- WalManager API 扩展

## 参考

- [Phase 0 设计文档](../analysis-segment-management.md)