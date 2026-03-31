# Easy WAL

基于 Kafka Log 模式的极简 WAL（Write-Ahead Log）库。

## 设计理念

**极简架构**：借鉴 Kafka Log 的设计思想，避免过度分层，状态集中管理。

**核心原则**：
- 单一入口：Wal 对象作为唯一对外接口
- 状态一致：读写共享同一个段对象
- 职责清晰：组件层只负责段内操作，协调层负责段管理决策

## 核心特性

- ✅ **简单直接**：2 层架构（WAL → LogSegment），无独立协调层
- ✅ **状态一致**：读写共享状态，消除同步问题
- ✅ **高性能**：内置 CRC32 优化和批量写入支持
- ✅ **可靠性强**：数据完整性校验和崩溃恢复
- ✅ **并发安全**：线程安全的实现，支持多线程并发读写
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
```

### 基本用法

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

## 在异步环境中使用 Wal

Easy WAL 提供同步接口，但可以轻松集成到异步环境中。根据你的异步运行时，选择合适的适配方式：

### Tokio 环境（推荐）

使用 `spawn_blocking` 在异步上下文中执行同步 WAL 操作：

```rust
use easy_wal::{Wal, Config, PersistenceMode};
use std::sync::Arc;
use tokio::task::spawn_blocking;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal = Arc::new(Wal::create("my_wal", Config::default())?);
    
    // 异步写入数据
    let wal_clone = Arc::clone(&wal);
    let data = b"Hello from async!".to_vec();
    let offset = spawn_blocking(move || {
        wal_clone.write(&data)
    }).await??;
    
    // 异步读取数据
    let wal_clone = Arc::clone(&wal);
    let read_data = spawn_blocking(move || {
        wal_clone.read(offset)
    }).await??;
    
    println!("Read: {:?}", String::from_utf8_lossy(&read_data));
    
    Ok(())
}
```

### 封装异步适配器

如果需要在多个地方使用，可以封装一个适配器：

```rust
use easy_wal::{Wal, Config, Error};
use std::sync::Arc;
use tokio::task::spawn_blocking;

/// 异步 WAL 适配器（Tokio 版本）
pub struct AsyncWalAdapter {
    inner: Arc<Wal>,
}

impl AsyncWalAdapter {
    pub fn create(path: impl AsRef<std::path::Path>, config: Config) -> Result<Self, Error> {
        Ok(Self {
            inner: Arc::new(Wal::create(path, config)?),
        })
    }
    
    pub async fn write(&self, data: &[u8]) -> Result<u64, Error> {
        let inner = Arc::clone(&self.inner);
        let data = data.to_vec();
        spawn_blocking(move || inner.write(&data))
            .await
            .map_err(|_| Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "spawn_blocking failed"
            )))?
    }
    
    pub async fn read(&self, offset: u64) -> Result<Vec<u8>, Error> {
        let inner = Arc::clone(&self.inner);
        spawn_blocking(move || inner.read(offset))
            .await
            .map_err(|_| Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "spawn_blocking failed"
            )))?
    }
    
    pub async fn flush(&self) -> Result<(), Error> {
        let inner = Arc::clone(&self.inner);
        spawn_blocking(move || inner.flush())
            .await
            .map_err(|_| Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "spawn_blocking failed"
            )))?
    }
}
```

### 其他异步运行时

**async-std**:
```rust
use async_std::task::spawn_blocking;
// 用法与 Tokio 类似
```

**smol**:
```rust
use smol::blocking;
// 使用 smol::blocking 替代 spawn_blocking
```

### 并发写入示例

```rust
use easy_wal::{Wal, Config, PersistenceMode};
use std::sync::Arc;
use tokio::task::spawn_blocking;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal = Arc::new(Wal::create(
        "concurrent_wal",
        Config::new().with_persistence_mode(PersistenceMode::Manual)
    )?);
    
    // 启动多个并发写入任务
    let mut tasks = vec![];
    for task_id in 0..5 {
        let wal_clone = Arc::clone(&wal);
        let task = tokio::spawn(async move {
            for i in 0..20 {
                let data = format!("Task {} - Record {}", task_id, i);
                let inner = Arc::clone(&wal_clone);
                spawn_blocking(move || inner.write(data.as_bytes()))
                    .await
                    .unwrap()
                    .unwrap();
            }
        });
        tasks.push(task);
    }
    
    // 等待所有任务完成
    for task in tasks {
        task.await?;
    }
    
    // 最终 flush
    let inner = Arc::clone(&wal);
    spawn_blocking(move || inner.flush()).await??;
    
    Ok(())
}
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

## 监控统计功能

Easy WAL 提供可选的监控统计功能，可通过 feature flag 控制开关。

### 启用统计功能

统计功能默认关闭，需要显式启用：

```toml
[dependencies]
easy_wal = { version = "0.1.0", features = ["stats"] }
```

### 使用统计功能

```rust
use easy_wal::{Wal, Config, WalStats};

// 创建 WAL（启用统计功能）
let wal = Wal::create("my_wal", Config::default())?;

// 写入数据
wal.write(b"record 1")?;
wal.write(b"record 2")?;

// 读取数据
wal.read(0)?;

// 刷新数据
wal.flush()?;

// 获取统计信息
let stats: WalStats = wal.stats();
println!("总记录数: {}", stats.total_records);
println!("总字节数: {}", stats.total_bytes);
println!("写入次数: {}", stats.write_count);
println!("读取次数: {}", stats.read_count);
println!("刷新次数: {}", stats.flush_count);
println!("段数量: {}", stats.segment_count);
```

### 统计指标说明

| 指标 | 说明 |
|------|------|
| `total_records` | 总记录数 |
| `total_bytes` | 总字节数（包含记录头） |
| `write_count` | 写入操作次数 |
| `read_count` | 读取操作次数 |
| `flush_count` | 刷新操作次数 |
| `segment_count` | 当前段数量 |

### 性能影响

- **启用统计**：使用 AtomicU64 + Relaxed ordering，性能开销极小（纳秒级）
- **禁用统计**：完全零开销，编译器会优化掉所有统计相关代码

### 适用场景

- ✅ **生产环境监控**：集成到 Prometheus/Grafana 等监控系统
- ✅ **性能分析**：定位性能瓶颈，优化配置参数
- ✅ **容量规划**：根据写入速率规划磁盘容量
- ❌ **嵌入式场景**：建议禁用统计，追求极致性能

## 性能指标

基于测试环境的性能参考（具体性能取决于硬件和场景）：

| 模式 | 吞吐量 | 适用场景 |
|------|--------|---------|
| Manual | 100+ MB/s | 最高性能，手动 flush |
| Batch | 50+ MB/s | 平衡方案，批量 flush |
| Immediate | 0.2 MB/s | 最高可靠性，每次写入 sync |

**注**：性能测试在并发场景下运行，孤立测试可达更高吞吐量。

## 测试覆盖

项目包含完整的测试套件：

- ✅ 67 个测试（默认），68 个测试（启用 stats），100% 通过率
- ✅ 功能测试：创建、写入、读取、flush、关闭
- ✅ 并发测试：多线程并发读写
- ✅ 崩溃恢复测试：数据完整性验证
- ✅ 段管理测试：轮转、清理、边界情况

## 参考设计

主要参考 Kafka Log 的段管理设计，同时借鉴：
- etcd/raft WAL 的简洁架构
- RocksDB WAL 的读写分离模式
- SQLite WAL 的可靠性设计

详细设计见 [`docs/architecture-design.md`](docs/architecture-design.md)。

## 开发状态

**Phase 2.6 已完成**：
- ✅ 同步 Wal 实现（稳定版本）
- ✅ 并发安全修复（段轮转保护）
- ✅ 锁结构优化（5 个独立锁 → 1 个 RwLock，消除死锁风险）
- ✅ mmap 性能优化（flush_range 精细刷新，性能提升 10000+ 倍）
- ✅ 完整测试覆盖（66 tests）

**性能改进**：
- **锁结构简化**：从 5 个独立锁简化到 1 个 RwLock + 1 个 AtomicBool，代码复杂度大幅降低
- **mmap 刷新优化**：从刷新整个 1GB mmap 到只刷新未刷新的数据（几十字节），性能提升 10000+ 倍
- **崩溃恢复改进**：修复 Batch 模式下可能丢失数据的 bug，确保所有数据都被正确刷新

**后续规划**：
- 📝 API 文档完善
- 📊 监控指标（Stats API）
- 🗜️ 压缩支持（Snappy/Zstd）
- 🚀 批量写入优化（write_batch API）

## 设计决策 FAQ

### 为什么只提供同步接口？

Easy WAL 选择只提供同步接口，这是基于以下考虑：

1. **主流实践**：RocksDB、LevelDB、SQLite 等主流 WAL 实现都采用同步接口
2. **简洁可靠**：同步模型更简单，更容易保证数据一致性和正确性
3. **灵活适配**：同步接口可以轻松适配到任何异步运行时（Tokio、async-std、smol）
4. **性能本质**：WAL 的性能瓶颈在磁盘 I/O，同步/异步的 CPU 开销差异可以忽略

### 如何在异步环境中使用？

使用异步运行时提供的 `spawn_blocking` 或 `blocking` 包装同步调用即可，如上面的示例所示。这种方式的性能与原生异步实现几乎相同，因为 WAL 操作本身就是 I/O 密集型。

## 许证

MIT License

## 贡献

欢迎提交 Issue 和 Pull Request！