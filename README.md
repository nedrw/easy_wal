# Easy WAL

基于 Kafka Log 模式的极简 WAL（Write-Ahead Log）库，提供同步和异步两种 API。

## 设计理念

**极简架构**：借鉴 Kafka Log 的设计思想，避免过度分层，状态集中管理。

**核心原则**：
- 单一入口：WAL/AsyncWal 对象作为唯一对外接口
- 状态一致：读写共享同一个段对象
- 职责清晰：组件层只负责段内操作，协调层负责段管理决策

## 核心特性

- ✅ **简单直接**：2 层架构（WAL → LogSegment），无独立协调层
- ✅ **状态一致**：读写共享状态，消除同步问题
- ✅ **高性能**：内置 CRC32 优化和批量写入支持
- ✅ **可靠性强**：数据完整性校验和崩溃恢复
- ✅ **并发安全**：线程安全的实现，支持多线程并发读写
- ✅ **异步支持**：提供 AsyncWal 异步 API，适用于高并发场景
- ✅ **多种持久化模式**：Immediate、Batch、Manual 三种模式满足不同需求
- ✅ **段自动轮转**：基于大小自动创建新段文件
- ✅ **旧段清理**：支持清理过期段文件，释放磁盘空间

## 快速开始

### 添加依赖

```toml
[dependencies]
easy_wal = "0.1.0"

[dev-dependencies]
tempfile = "*"  # 用于测试
tokio = { version = "1", features = ["full"] }  # 如果使用 AsyncWal
```

### 同步 Wal 基本用法

```rust
use easy_wal::{Wal, Config, PersistenceMode};
use tempfile::TempDir;

let temp_dir = TempDir::new().unwrap();
let wal_path = temp_dir.path().join("wal");

// 创建 WAL（使用 Manual 模式，性能最优）
let config = Config::new()
    .with_persistence_mode(PersistenceMode::Manual)
    .with_segment_size(1024 * 1024); // 1MB

let wal = Wal::create(&wal_path, config).unwrap();

// 写入数据
let data = b"Hello, WAL!";
let offset = wal.write(data).unwrap();

// 手动 flush（Manual 模式需要显式调用）
wal.flush().unwrap();

// 读取数据
let read_data = wal.read(offset).unwrap();
assert_eq!(read_data, data);

// 清理旧段（可选）
wal.prune_segments(100).unwrap(); // 保留 offset >= 100 的数据

// 关闭 WAL
wal.close().unwrap();
```

### 异步 AsyncWal 基本用法

```rust
use easy_wal::{AsyncWal, Config, PersistenceMode};
use tempfile::TempDir;

let temp_dir = TempDir::new().unwrap();
let wal_path = temp_dir.path().join("async_wal");

// 创建 AsyncWal
let config = Config::new()
    .with_persistence_mode(PersistenceMode::Manual);

let wal = AsyncWal::create(&wal_path, config).await.unwrap();

// 异步写入数据
let data = b"Hello, Async WAL!";
let offset = wal.write(data).await.unwrap();

// 异步 flush
wal.flush().await.unwrap();

// 异步读取数据
let read_data = wal.read(offset).await.unwrap();
assert_eq!(read_data, data);

// 异步关闭
wal.close().await.unwrap();
```

### 异步并发写入示例

```rust
use easy_wal::{AsyncWal, Config, PersistenceMode};
use std::sync::Arc;
use tokio::task;
use tempfile::TempDir;

let temp_dir = TempDir::new().unwrap();
let wal_path = temp_dir.path().join("concurrent_wal");

let config = Config::new()
    .with_persistence_mode(PersistenceMode::Manual)
    .with_segment_size(10 * 1024 * 1024); // 10MB

let wal = Arc::new(AsyncWal::create(&wal_path, config).await.unwrap());

// 启动多个并发写入任务
let mut tasks = vec![];
for task_id in 0..5 {
    let wal_clone = Arc::clone(&wal);
    let task = task::spawn(async move {
        for i in 0..20 {
            let data = format!("Task {} - Record {}", task_id, i);
            wal_clone.write(data.as_bytes()).await.unwrap();
        }
    });
    tasks.push(task);
}

// 等待所有任务完成
for task in tasks {
    task.await.unwrap();
}

// 最终 flush
wal.flush().await.unwrap();
```

## 持久化模式

Easy WAL 提供三种持久化模式，满足不同的性能和可靠性需求：

### Immediate 模式（最高可靠性）

每次写入都立即 sync 到磁盘，确保数据不会丢失。

```rust
let config = Config::new()
    .with_persistence_mode(PersistenceMode::Immediate);
```

**特点**：
- ✅ 最高可靠性，每次写入都持久化
- ❌ 性能较低（每次写入都 sync）
- 适用场景：关键数据、金融交易、审计日志

### Batch 模式（平衡方案）

批量写入，手动调用 flush 持久化。

```rust
let config = Config::new()
    .with_persistence_mode(PersistenceMode::Batch);

// 批量写入
for i in 0..100 {
    wal.write(format!("Data {}", i).as_bytes()).unwrap();
}

// 手动 flush（批量持久化）
wal.flush().unwrap();
```

**特点**：
- ✅ 平衡性能和可靠性
- ✅ 批量持久化减少 sync 次数
- 适用场景：一般应用、日志收集、数据流处理

### Manual 模式（最高性能）

完全手动控制 flush，性能最优。

```rust
let config = Config::new()
    .with_persistence_mode(PersistenceMode::Manual);

// 写入大量数据
for i in 0..1000 {
    wal.write(format!("Data {}", i).as_bytes()).unwrap();
}

// 只 flush 一次（性能最优）
wal.flush().unwrap();
```

**特点**：
- ✅ 最高性能（最小化 sync 操作）
- ❌ 需要手动管理 flush，可能丢失数据
- 适用场景：高性能场景、批量导入、临时数据

## API 选择指南

### 使用同步 Wal 的场景

- 单线程应用
- 简单的日志记录
- 不需要高并发
- 快速原型开发

### 使用异步 AsyncWal 的场景

- 高并发应用（Web 服务、API 服务）
- 需要非阻塞 I/O
- Tokio 异步 runtime 环境
- 多任务并发写入

## 性能指标

基于测试环境的性能参考（具体性能取决于硬件和场景）：

| 模式 | 同步 Wal | 异步 AsyncWal |
|------|----------|---------------|
| Manual | 100+ MB/s | 100+ MB/s |
| Batch | 50+ MB/s | 50+ MB/s |
| Immediate | 0.2 MB/s | 0.2 MB/s |

**注**：性能测试在并发场景下运行，孤立测试可达更高吞吐量。

## 更多示例

查看 `examples/` 目录中的完整示例：

- `examples/async_wal_example.rs` - AsyncWal 综合示例
  - 基本用法
  - 持久化模式对比
  - 并发写入
  - 重新打开 WAL

## 测试覆盖

项目包含完整的测试套件：

- ✅ 100 个测试，100% 通过率
- ✅ 功能测试：创建、写入、读取、flush、关闭
- ✅ 并发测试：多线程并发读写
- ✅ 崩溃恢复测试：数据完整性验证
- ✅ 性能测试：吞吐量和延迟测试
- ✅ 段管理测试：轮转、清理、边界情况

## 参考设计

主要参考 Kafka Log 的段管理设计，同时借鉴：
- etcd/raft WAL 的简洁架构
- RocksDB WAL 的读写分离模式
- SQLite WAL 的可靠性设计

详细设计见 [`docs/architecture-design.md`](docs/architecture-design.md)。

## 开发状态

**Phase 3 已完成**：
- ✅ 同步 Wal 实现（稳定版本）
- ✅ 异步 AsyncWal 实现（新增）
- ✅ 并发安全修复（段轮转保护）
- ✅ 完整测试覆盖（100 tests）

**后续规划**：
- 📝 API 文档完善
- 🚀 性能优化（内存映射、压缩）
- 🔧 Auto-flush 功能（Batch 模式增强）

## 许证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！