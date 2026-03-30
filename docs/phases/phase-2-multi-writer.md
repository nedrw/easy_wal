# Phase 2: 统一写入架构设计方案

## 1. 目标与背景

### 1.1 目标

将 CommitCoordinator 作为唯一写入路径，实现智能退化机制，统一单写和多写模式，简化架构。

### 1.2 方案选择

**方案对比：**

| 方案 | 架构复杂度 | 维护成本 | 性能 | 用户体验 |
|------|-----------|---------|------|---------|
| **方案A**：双协调器 | 高（两个协调器） | 高（两套代码） | 需要手动选择 | 困惑（何时用哪个？） |
| **方案B**：统一协调器（选中） | 低（单一协调器） | 低（一套代码） | 自适应优化 | 简单（无需关心模式） |

**方案B 核心思想：**
1. **唯一写入路径**：所有写入都通过 CommitCoordinator
2. **智能退化**：单 writer 时自动退化到直接写入（无需 Group Commit 开销）
3. **架构简化**：移除 WriteCoordinator，WalManager 直接使用 CommitCoordinator
4. **性能自适应**：根据负载自动选择最优策略

### 1.3 背景分析

**当前架构（Phase 0-1）：**
```
WalManager
├── WriteCoordinator (单写协调器)
│   └── SegmentCoordinator
│       └── LogWriter
└── CommitCoordinator (已实现，未使用)
    └── SegmentCoordinator
        └── LogWriter
```

**问题：**
- 两套协调器导致职责重叠和代码重复
- 用户需要手动选择模式（单写 vs 多写）
- 维护成本高（两套代码路径）

**Phase 2 目标架构：**
```
WalManager
├── CommitCoordinator (唯一协调器)
│   ├── 智能退化机制
│   │   ├── 单 writer → 直接写入（无 Group Commit 开销）
│   │   └── 多 writer → Group Commit（高性能）
│   └── SegmentCoordinator
│       └── LogWriter
├── WriterRegistry (新增：Writer 注册表)
├── ReadCoordinator (不变)
└── RecoveryManager (不变)
```

### 1.4 核心价值

1. **架构最简**：单一协调器，职责清晰
2. **性能自适应**：根据负载自动优化
3. **维护成本低**：一套代码路径，无重复
4. **用户体验佳**：无需关心模式，API 统一
5. **工作量减少**：预计 3-5 天（vs 原方案 1-2 周）

---

## 2. 架构设计

### 2.1 整体架构

```
┌──────────────────────────────────────────────────────────┐
│                      WalManager                          │
│  ┌────────────────────────────────────────────────┐     │
│  │         CommitCoordinator (统一协调器)         │     │
│  │  ┌──────────────────────────────────────────┐ │     │
│  │  │  WriterRegistry                          │ │     │
│  │  │  - writer_1 → WriterHandle              │ │     │
│  │  │  - writer_2 → WriterHandle              │ │     │
│  │  └──────────────────────────────────────────┘ │     │
│  │  ┌──────────────────────────────────────────┐ │     │
│  │  │  Intelligent Degradation                 │ │     │
│  │  │  - single_writer → direct write         │ │     │
│  │  │  - multi_writer → group commit          │ │     │
│  │  └──────────────────────────────────────────┘ │     │
│  │  ┌──────────────────────────────────────────┐ │     │
│  │  │  Group Commit Core (Phase 1)             │ │     │
│  │  │  - Batch Collection                     │ │     │
│  │  │  - Batch Merge & Flush                  │ │     │
│  │  └──────────────────────────────────────────┘ │     │
│  │  ┌──────────────────────────────────────────┐ │     │
│  │  │  SegmentCoordinator (Phase 0)           │ │     │
│  │  │  - Segment Rotation                     │ │     │
│  │  │  - Active Writer Management             │ │     │
│  │  └──────────────────────────────────────────┘ │     │
│  └────────────────────────────────────────────────┘     │
│  ┌────────────────────────────────────────────────┐     │
│  │         ReadCoordinator                        │     │
│  └────────────────────────────────────────────────┘     │
│  ┌────────────────────────────────────────────────┐     │
│  │         RecoveryManager                         │     │
│  └────────────────────────────────────────────────┘     │
└──────────────────────────────────────────────────────────┘
```

### 2.2 数据流

```
Writer1 ──┐
Writer2 ──┼──► WriterHandle.write(data)
Writer3 ──┘            │
                       ▼
        CommitCoordinator.add_batch(batch)
                       │
                       ▼
          ┌─────────────────────────┐
          │  Writer Count Check      │
          │  - single → direct write │
          │  - multi → group commit  │
          └─────────────────────────┘
                       │
         ┌─────────────┴─────────────┐
         ▼                           ▼
   Direct Write Path         Group Commit Path
         │                           │
         │                  ┌─────────────────┐
         │                  │ Commit Loop      │
         │                  │ - Collect batches│
         │                  │ - Merge records  │
         │                  │ - Single fsync  │
         │                  └─────────────────┘
         │                           │
         └─────────────┬─────────────┘
                       ▼
          SegmentCoordinator.get_active_writer()
                       │
                       ▼
                  LogWriter.write_batch()
                       │
                       ▼
                    fsync()
                       │
                       ▼
          batch.send_result(positions)
                       │
                       ▼
          WriterHandle receives result
```

### 2.3 组件职责

| 组件 | 职责 | 关键方法 |
|------|------|----------|
| **CommitCoordinator** | 统一写入协调器，智能退化，Group Commit | `add_batch()`, `flush()`, `shutdown()`, `register_writer()` |
| **WriterRegistry** | Writer 注册表，管理 WriterHandle | `register()`, `unregister()`, `count()` |
| **WriterHandle** | Writer 句柄，提供写入接口 | `write()`, `write_batch()`, `close()` |
| **SegmentCoordinator** | 段轮转决策（Phase 0） | `get_active_writer()`, `check_and_rotate()` |
| **ReadCoordinator** | 读取协调（不变） | `read()`, `read_batch()` |
| **RecoveryManager** | 恢复管理（不变） | `recover()` |

---

## 3. 核心组件设计

### 3.1 CommitCoordinator 扩展

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use tokio::sync::RwLock;
use crate::prelude::*;

/// Writer 标识符
pub type WriterId = u64;

/// CommitCoordinator 扩展
pub struct CommitCoordinator {
    /// 配置
    config: CommitConfig,
    /// 下一个序列号（原子）
    next_sequence: AtomicU64,
    /// 内部状态
    state: RwLock<CoordinatorState>,
    /// 用于唤醒 commit loop 的信号
    commit_wakeup: Arc<tokio::sync::Notify>,
    /// SegmentCoordinator 引用
    segment_coordinator: Arc<SegmentCoordinator>,
    /// 统计信息
    stats: Arc<RwLock<CommitStats>>,
    /// Writer 注册表
    writer_registry: WriterRegistry,
    /// 模式：单写优化标志
    single_writer_mode: AtomicBool,
}

/// Writer 注册表
pub struct WriterRegistry {
    /// 注册的 writers
    writers: RwLock<HashMap<WriterId, Arc<WriterMeta>>>,
    /// 下一个 Writer ID
    next_writer_id: AtomicU64,
}

/// Writer 元数据
#[derive(Debug)]
pub struct WriterMeta {
    pub id: WriterId,
    pub name: Option<String>,
    pub created_at: std::time::Instant,
    pub stats: WriterStats,
}

/// Writer 统计
#[derive(Debug, Clone, Default)]
pub struct WriterStats {
    pub write_count: u64,
    pub write_bytes: u64,
    pub write_records: u64,
}

impl CommitCoordinator {
    /// 创建新的 CommitCoordinator
    pub async fn new(
        config: CommitConfig,
        segment_coordinator: Arc<SegmentCoordinator>,
    ) -> Result<Self> {
        Ok(Self {
            config,
            next_sequence: AtomicU64::new(0),
            state: RwLock::new(CoordinatorState::default()),
            commit_wakeup: Arc::new(tokio::sync::Notify::new()),
            segment_coordinator,
            stats: Arc::new(RwLock::new(CommitStats::default())),
            writer_registry: WriterRegistry::new(),
            single_writer_mode: AtomicBool::new(true),
        })
    }
    
    /// 启动协调器（后台任务）
    pub fn start(&self) {
        let this = self.clone();
        tokio::spawn(async move {
            this.commit_loop().await;
        });
    }
    
    /// 注册 writer，返回 WriterHandle
    pub async fn register_writer(&self, name: Option<String>) -> Result<WriterHandle> {
        let (writer_id, meta) = self.writer_registry.register(name).await?;
        
        // 检查是否需要切换模式
        let writer_count = self.writer_registry.count().await;
        if writer_count > 1 {
            self.single_writer_mode.store(false, Ordering::Release);
            info!("Switched to multi-writer mode ({} writers)", writer_count);
        }
        
        Ok(WriterHandle::new(
            writer_id,
            self.clone(),
            self.stats.clone(),
            meta,
        ))
    }
    
    /// 注销 writer
    pub async fn unregister_writer(&self, writer_id: WriterId) -> Result<()> {
        self.writer_registry.unregister(writer_id).await?;
        
        // 检查是否需要切换回单写模式
        let writer_count = self.writer_registry.count().await;
        if writer_count <= 1 {
            self.single_writer_mode.store(true, Ordering::Release);
            info!("Switched to single-writer mode ({} writers)", writer_count);
        }
        
        Ok(())
    }
    
    /// 添加批次到提交队列
    pub async fn add_batch(&self, batch: Arc<WriteBatch>) {
        // 单写模式优化：直接处理
        if self.single_writer_mode.load(Ordering::Acquire) {
            self.direct_write(batch).await;
            return;
        }
        
        // 多写模式：加入队列，等待 Group Commit
        {
            let mut state = self.state.write().await;
            state.pending.push(batch);
        }
        // 通知 commit loop
        self.commit_wakeup.notify_one();
    }
    
    /// 直接写入（单写模式优化）
    async fn direct_write(&self, batch: Arc<WriteBatch>) {
        // 获取活跃写入器
        let writer = match self.segment_coordinator.get_active_writer().await {
            Ok(w) => w,
            Err(e) => {
                batch.send_result(Err(e));
                return;
            }
        };
        
        // 执行写入
        let records: Vec<&[u8]> = batch.records.iter().map(|r| r.as_slice()).collect();
        let positions = match writer.write_batch(&records).await {
            Ok(pos) => pos,
            Err(e) => {
                batch.send_result(Err(e));
                return;
            }
        };
        
        // 同步
        if let Err(e) = writer.sync().await {
            batch.send_result(Err(e));
            return;
        }
        
        // 更新段大小
        let bytes_written = batch.size_bytes as u64 + batch.records.len() as u64 * 12;
        self.segment_coordinator.update_size(bytes_written, batch.records.len() as u64).await;
        
        // 检查轮转
        let _ = self.segment_coordinator.check_and_rotate().await;
        
        // 发送结果
        batch.send_result(Ok(positions));
        
        // 更新统计
        let mut stats = self.stats.write().await;
        stats.total_batches += 1;
        stats.total_records += batch.records.len() as u64;
        stats.total_bytes += batch.size_bytes as u64;
        stats.single_commits += 1;
    }
    
    /// 提交循环（多写模式）
    async fn commit_loop(&self) {
        loop {
            // 等待条件满足或超时
            {
                let state = self.state.read().await;
                if state.pending.is_empty() && !state.shutdown {
                    // 等待新批次或超时
                    tokio::select! {
                        _ = self.commit_wakeup.notified() => {}
                        _ = tokio::time::sleep(Duration::from_millis(self.config.max_wait_time_ms)) => {}
                    }
                }
            }
            
            // 检查关闭状态并取出待处理的批次
            let pending = {
                let mut state = self.state.write().await;
                if state.shutdown && state.pending.is_empty() {
                    break;
                }
                
                // 检查是否需要提交
                let should_commit = Self::should_commit(&state.pending, &self.config);
                
                if should_commit && !state.pending.is_empty() {
                    // 取走所有待处理的批次
                    let batches = std::mem::take(&mut state.pending);
                    batches
                } else {
                    // 继续等待
                    continue;
                }
            };
            
            // 处理批次
            if let Err(e) = self.flush_batches(pending).await {
                error!("Batch flush failed: {:?}", e);
            }
        }
    }
    
    /// 获取活跃 writer 数量
    pub async fn writer_count(&self) -> u64 {
        self.writer_registry.count().await
    }
    
    /// 获取当前模式
    pub fn mode(&self) -> WriteMode {
        if self.single_writer_mode.load(Ordering::Acquire) {
            WriteMode::Single
        } else {
            WriteMode::Multi
        }
    }
}

impl WriterRegistry {
    fn new() -> Self {
        Self {
            writers: RwLock::new(HashMap::new()),
            next_writer_id: AtomicU64::new(1),
        }
    }
    
    async fn register(&self, name: Option<String>) -> Result<(WriterId, Arc<WriterMeta>)> {
        let writer_id = self.next_writer_id.fetch_add(1, Ordering::AcqRel);
        
        let meta = Arc::new(WriterMeta {
            id: writer_id,
            name,
            created_at: std::time::Instant::now(),
            stats: WriterStats::default(),
        });
        
        {
            let mut writers = self.writers.write().await;
            writers.insert(writer_id, meta.clone());
        }
        
        info!("Writer {} registered", writer_id);
        Ok((writer_id, meta))
    }
    
    async fn unregister(&self, writer_id: WriterId) -> Result<()> {
        let mut writers = self.writers.write().await;
        if writers.remove(&writer_id).is_some() {
            info!("Writer {} unregistered", writer_id);
        }
        Ok(())
    }
    
    async fn count(&self) -> u64 {
        let writers = self.writers.read().await;
        writers.len() as u64
    }
}

/// 写入模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// 单写模式（直接写入，无 Group Commit 开销）
    Single,
    /// 多写模式（Group Commit）
    Multi,
}
```

### 3.2 WriterHandle

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use tokio::sync::RwLock;

/// Writer 句柄
///
/// 代表一个独立的 writer，提供写入接口。
/// 可以被单个线程或任务持有。
pub struct WriterHandle {
    /// Writer ID
    writer_id: WriterId,
    /// CommitCoordinator 引用
    coordinator: Arc<CommitCoordinator>,
    /// 全局统计信息引用
    stats: Arc<RwLock<CommitStats>>,
    /// Writer 元数据
    meta: Arc<WriterMeta>,
    /// 本地统计（用于快速更新）
    local_stats: WriterStats,
    /// 关闭标志
    closed: AtomicBool,
}

impl WriterHandle {
    /// 创建新的 WriterHandle（内部方法）
    fn new(
        writer_id: WriterId,
        coordinator: Arc<CommitCoordinator>,
        stats: Arc<RwLock<CommitStats>>,
        meta: Arc<WriterMeta>,
    ) -> Self {
        Self {
            writer_id,
            coordinator,
            stats,
            meta,
            local_stats: WriterStats::default(),
            closed: AtomicBool::new(false),
        }
    }
    
    /// 获取 Writer ID
    pub fn id(&self) -> WriterId {
        self.writer_id
    }
    
    /// 写入单条数据
    pub async fn write(&self, data: &[u8]) -> Result<WritePosition> {
        self.write_batch(&[data]).await.map(|mut v| v.remove(0))
    }
    
    /// 批量写入
    pub async fn write_batch(&self, data_list: &[&[u8]]) -> Result<Vec<WritePosition>> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Generic("Writer is closed".to_string()));
        }
        
        if data_list.is_empty() {
            return Ok(Vec::new());
        }
        
        // 创建 WriteBatch
        let records: Vec<Vec<u8>> = data_list.iter().map(|d| d.to_vec()).collect();
        let (batch, result_rx) = WriteBatch::new(self.writer_id, records);
        
        // 提交到 CommitCoordinator
        self.coordinator.add_batch(batch).await;
        
        // 等待结果
        let positions = result_rx
            .await
            .map_err(|_| Error::Generic("Commit channel closed".to_string()))??;
        
        // 更新统计
        self.update_stats(data_list).await;
        
        Ok(positions)
    }
    
    /// 更新统计信息
    async fn update_stats(&self, data_list: &[&[u8]]) {
        // 更新本地统计
        self.local_stats.write_count += 1;
        self.local_stats.write_records += data_list.len() as u64;
        self.local_stats.write_bytes += data_list.iter().map(|d| d.len() as u64).sum::<u64>();
        
        // 定期更新全局统计（避免频繁锁竞争）
        if self.local_stats.write_count % 10 == 0 {
            let mut stats = self.stats.write().await;
            stats.total_writes += self.local_stats.write_count;
            stats.total_records += self.local_stats.write_records;
            stats.total_bytes += self.local_stats.write_bytes;
            
            // 重置本地统计
            self.local_stats = WriterStats::default();
        }
    }
    
    /// 关闭 writer
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(()); // 已经关闭
        }
        
        // 从注册表注销
        self.coordinator.unregister_writer(self.writer_id).await?;
        
        // 刷新剩余统计
        let mut stats = self.stats.write().await;
        stats.total_writes += self.local_stats.write_count;
        stats.total_records += self.local_stats.write_records;
        stats.total_bytes += self.local_stats.write_bytes;
        
        debug!("Writer {} closed", self.writer_id);
        Ok(())
    }
}

impl Drop for WriterHandle {
    fn drop(&mut self) {
        // 标记为关闭
        self.closed.store(true, Ordering::Release);
    }
}
```

### 3.3 WalManager 简化

```rust
/// WAL 管理器（简化版）
///
/// 统一使用 CommitCoordinator，无需区分单写/多写模式。
pub struct WalManager {
    /// CommitCoordinator（唯一协调器）
    commit_coordinator: Arc<CommitCoordinator>,
    
    /// ReadCoordinator
    read_coordinator: Arc<ReadCoordinator>,
    
    /// RecoveryManager
    recovery_manager: RecoveryManager,
    
    /// 配置
    config: WalConfig,
    
    /// 关闭信号
    shutdown_tx: Option<tokio::sync::watch::Sender<bool>>,
}

impl WalManager {
    /// 创建 WAL 管理器
    pub async fn new(config: WalConfig) -> Result<Self> {
        // 创建目录
        tokio::fs::create_dir_all(&config.dir).await?;
        
        // 创建段协调器
        let rotation_config = RotationConfig::new().with_max_size(config.max_segment_size);
        let segment_config = SegmentConfig::new(&config.dir);
        let segment_coordinator = Arc::new(
            SegmentCoordinator::new(rotation_config, segment_config).await?
        );
        
        // 创建 CommitCoordinator（统一协调器）
        let commit_config = config.commit_config.unwrap_or_default();
        let commit_coordinator = Arc::new(
            CommitCoordinator::new(commit_config, segment_coordinator).await?
        );
        
        // 启动 commit loop
        commit_coordinator.start();
        
        // 创建读取器和协调器
        let reader_config = LogReaderConfig::default()
            .with_dir(&config.dir)
            .with_batch_size(config.batch_size);
        let reader = Arc::new(tokio::sync::RwLock::new(
            LogReader::new(reader_config).await?,
        ));
        let read_coordinator = Arc::new(
            ReadCoordinator::new(reader).with_read_ahead(config.read_ahead_size)
        );
        
        // 创建恢复管理器
        let recovery_manager = RecoveryManager::new(&config.dir);
        
        Ok(Self {
            commit_coordinator,
            read_coordinator,
            recovery_manager,
            config,
            shutdown_tx: None,
        })
    }
    
    /// 注册 writer，返回 WriterHandle
    ///
    /// 单个 writer 时自动优化为直接写入，
    /// 多个 writer 时自动启用 Group Commit。
    pub async fn register_writer(&self, name: Option<String>) -> Result<WriterHandle> {
        self.commit_coordinator.register_writer(name).await
    }
    
    /// 写入单条数据（便捷方法）
    ///
    /// 内部创建临时 writer，适用于简单场景。
    /// 高性能场景建议使用 `register_writer()` 获取持久化 writer。
    pub async fn write(&self, data: &[u8]) -> Result<WritePosition> {
        let writer = self.register_writer(None).await?;
        let pos = writer.write(data).await?;
        writer.close().await?;
        Ok(pos)
    }
    
    /// 批量写入（便捷方法）
    pub async fn write_batch(&self, data_list: &[&[u8]]) -> Result<Vec<WritePosition>> {
        let writer = self.register_writer(None).await?;
        let positions = writer.write_batch(data_list).await?;
        writer.close().await?;
        Ok(positions)
    }
    
    /// 获取当前模式
    pub fn write_mode(&self) -> WriteMode {
        self.commit_coordinator.mode()
    }
    
    /// 获取活跃 writer 数量
    pub async fn writer_count(&self) -> u64 {
        self.commit_coordinator.writer_count().await
    }
    
    /// 获取统计信息
    pub async fn stats(&self) -> CommitStats {
        self.commit_coordinator.stats().await
    }
    
    /// 强制刷新
    pub async fn flush(&self) -> Result<()> {
        self.commit_coordinator.flush().await
    }
    
    // ... 其他方法（read, seek, recover 等）保持不变 ...
}
```

### 3.4 WalBuilder 简化

```rust
impl WalBuilder {
    /// 配置 Group Commit 参数（可选）
    pub fn with_commit_config(mut self, config: CommitConfig) -> Self {
        self.config.commit_config = Some(config);
        self
    }
    
    /// 构建 WalManager
    pub async fn build(&self) -> Result<WalManager> {
        WalManager::new(self.config.clone()).await
    }
}
```

---

## 4. 使用示例

### 4.1 基本用法（自动优化）

```rust
// 创建 WAL（无需指定模式）
let wal = WalBuilder::new()
    .with_dir("./wal_data")
    .with_commit_config(CommitConfig {
        max_batch_size: 64 * 1024,
        max_wait_time_ms: 5,
        max_batch_count: 100,
        min_batches_for_commit: 2,
    })
    .build()
    .await?;

// 单 writer：自动优化为直接写入
let writer = wal.register_writer(Some("writer-1".to_string())).await?;
writer.write(b"data").await?;
writer.close().await?;

// 多 writer：自动启用 Group Commit
let w1 = wal.register_writer(Some("w1".to_string())).await?;
let w2 = wal.register_writer(Some("w2".to_string())).await?;

// 并发写入（自动 Group Commit）
tokio::spawn(async move {
    w1.write(b"data-1").await?;
    w1.close().await
});

tokio::spawn(async move {
    w2.write(b"data-2").await?;
    w2.close().await
});

// 便捷方法（临时 writer）
wal.write(b"simple-data").await?;
wal.write_batch(&[b"a", b"b"]).await?;
```

### 4.2 模式切换观察

```rust
let wal = WalBuilder::new()
    .with_dir("./wal_data")
    .build()
    .await?;

// 初始：单写模式
assert_eq!(wal.write_mode(), WriteMode::Single);

let w1 = wal.register_writer(None).await?;
assert_eq!(wal.write_mode(), WriteMode::Single);

// 注册第二个 writer：切换到多写模式
let w2 = wal.register_writer(None).await?;
assert_eq!(wal.write_mode(), WriteMode::Multi);

// 关闭一个 writer：切换回单写模式
w2.close().await?;
assert_eq!(wal.write_mode(), WriteMode::Single);
```

### 4.3 性能对比

```rust
#[tokio::test]
async fn benchmark_single_vs_multi_writer() {
    let temp_dir = tempfile::tempdir().unwrap();
    
    // 单 writer（自动直接写入）
    let start = std::time::Instant::now();
    let wal = WalBuilder::new()
        .with_dir(temp_dir.path())
        .build()
        .await?;
    
    let writer = wal.register_writer(None).await?;
    for _ in 0..10000 {
        writer.write(b"data").await?;
    }
    writer.close().await?;
    wal.close().await?;
    
    let single_time = start.elapsed();
    
    // 多 writer（自动 Group Commit）
    let start = std::time::Instant::now();
    let wal = WalBuilder::new()
        .with_dir(temp_dir.path())
        .build()
        .await?;
    
    let mut handles = vec![];
    for _ in 0..10 {
        let writer = wal.register_writer(None).await?;
        let handle = tokio::spawn(async move {
            for _ in 0..1000 {
                writer.write(b"data").await?;
            }
            writer.close().await
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.await??;
    }
    
    wal.close().await?;
    let multi_time = start.elapsed();
    
    println!("Single-writer: {:?}", single_time);
    println!("Multi-writer: {:?}", multi_time);
    
    // 多写模式应该更快（Group Commit 减少 fsync）
}
```

---

## 5. 实现步骤

### 5.1 Task 2.1: 扩展 CommitCoordinator（2 天）

**步骤：**

1. **添加 WriterRegistry**
   - 实现 `WriterRegistry` 结构体
   - 实现 `register()`, `unregister()`, `count()` 方法
   - 使用 `RwLock<HashMap<WriterId, Arc<WriterMeta>>>` 管理注册表

2. **实现智能退化机制**
   - 添加 `single_writer_mode: AtomicBool` 标志
   - 实现 `register_writer()`, `unregister_writer()` 方法
   - 在注册/注销时检查并切换模式
   - 实现 `direct_write()` 方法（单写优化）

3. **修改 add_batch() 逻辑**
   - 检查 `single_writer_mode` 标志
   - 单写模式：调用 `direct_write()`
   - 多写模式：加入队列，等待 Group Commit

4. **添加 WriteMode 枚举**
   - 定义 `WriteMode::Single` 和 `WriteMode::Multi`
   - 实现 `mode()` 方法返回当前模式

5. **测试**
   - 单 writer 注册/注销测试
   - 模式切换测试
   - 单写模式直接写入测试
   - 多写模式 Group Commit 测试

**关键代码位置：** `src/wal/commit_coordinator.rs`

### 5.2 Task 2.2: 实现 WriterHandle（1 天）

**步骤：**

1. **实现 WriterHandle 结构体**
   - `write()`, `write_batch()` 方法
   - 结果等待机制（基于 `oneshot` channel）
   - 统计信息更新

2. **实现关闭逻辑**
   - `close()` 方法
   - 从注册表注销
   - 刷新统计信息
   - `Drop` trait 实现

3. **测试**
   - 单 writer 写入测试
   - 批量写入测试
   - 关闭和清理测试

**关键代码位置：** `src/wal/commit_coordinator.rs`（同一文件）

### 5.3 Task 2.3: 简化 WalManager（1 天）

**步骤：**

1. **移除 WriteCoordinator**
   - 删除 `WriteCoordinator` 字段
   - 删除相关导入和代码

2. **使用 CommitCoordinator**
   - 添加 `commit_coordinator: Arc<CommitCoordinator>` 字段
   - 修改 `new()` 方法创建 CommitCoordinator

3. **实现 register_writer() 方法**
   - 直接调用 `commit_coordinator.register_writer()`

4. **实现便捷方法**
   - `write()`, `write_batch()` 创建临时 writer

5. **测试**
   - 单写模式测试
   - 多写模式测试
   - 模式切换测试
   - 向后兼容性测试

**关键代码位置：** `src/wal/wal_manager.rs`

### 5.4 Task 2.4: 集成测试（1 天）

**测试场景：**

1. **并发写入测试**
   - 10 个 writer 并发写入
   - 每个写入 1000 条记录
   - 验证数据完整性
   - 验证序列号唯一性

2. **性能测试**
   - 单写模式性能基准
   - 多写模式性能基准
   - 对比直接写入 vs Group Commit

3. **模式切换测试**
   - 单 writer → 多 writer → 单 writer
   - 验证模式切换正确性
   - 验证无数据丢失

4. **压力测试**
   - 100 个 writer 并发注册/注销
   - 持续写入 1 分钟
   - 监控内存和 CPU

**关键代码位置：** `tests/integration_test.rs`

---

## 6. 性能分析

### 6.1 单写模式性能

**直接写入路径：**
```
Writer → CommitCoordinator.add_batch()
       → direct_write() [单写优化]
       → SegmentCoordinator.get_active_writer()
       → LogWriter.write_batch()
       → fsync()
       → 返回结果
```

**性能特点：**
- 无队列等待开销
- 无批次合并开销
- 直接写入 + fsync
- **性能等同于原 WriteCoordinator**

### 6.2 多写模式性能

**Group Commit 路径：**
```
Writers → CommitCoordinator.add_batch()
        → 加入队列
        → Commit Loop 等待条件
        → 批次合并
        → SegmentCoordinator.get_active_writer()
        → LogWriter.write_batch()
        → 单次 fsync
        → 返回结果
```

**性能特点：**
- 多个批次共享一次 fsync
- 批次合并减少 I/O 次数
- **性能提升：5-10x（取决于并发度）**

### 6.3 模式切换开销

**单写 → 多写：**
- 设置 `single_writer_mode = false`
- 无需等待，立即生效
- **开销：原子操作（纳秒级）**

**多写 → 单写：**
- 设置 `single_writer_mode = true`
- Commit Loop 自然结束当前批次
- **开销：原子操作（纳秒级）**

### 6.4 性能目标

| 场景 | 目标 | 实测 |
|------|------|------|
| 单写模式吞吐量 | ≥ 50k QPS | 待测 |
| 多写模式吞吐量（10 writers） | ≥ 100k QPS | 待测 |
| 模式切换延迟 | < 1μs | 待测 |
| 内存占用（10k writers） | < 100MB | 待测 |

---

## 7. 向后兼容性

### 7.1 API 兼容

**旧 API（WriteCoordinator）：**
```rust
// 已废弃
wal.write(data).await?;
wal.write_batch(&[data1, data2]).await?;
```

**新 API（CommitCoordinator）：**
```rust
// 便捷方法（兼容）
wal.write(data).await?;
wal.write_batch(&[data1, data2]).await?;

// 高性能方法（新增）
let writer = wal.register_writer(None).await?;
writer.write(data).await?;
writer.close().await?;
```

**迁移策略：**
- 旧 API 保持兼容，内部创建临时 writer
- 文档建议高性能场景使用 `register_writer()`
- 无需修改现有代码

### 7.2 数据格式兼容

- WAL 文件格式不变
- 段格式不变
- 恢复逻辑不变

### 7.3 性能兼容

- 单写模式性能不受影响（智能退化）
- 多写模式提供额外性能提升

---

## 8. 风险与缓解

### 8.1 技术风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| **模式切换竞争** | 数据竞争 | 原子操作，无锁切换 |
| **性能退化** | 单写模式变慢 | 充分测试，基准对比 |
| **恢复逻辑复杂** | 数据丢失 | 基于现有 RecoveryManager |
| **统计信息不准** | 监控失真 | 本地聚合 + 定期同步 |

### 8.2 缓解策略

1. **充分测试**
   - 单写模式性能测试
   - 多写模式性能测试
   - 模式切换测试
   - 边界条件测试

2. **基准对比**
   - 与原 WriteCoordinator 性能对比
   - 确保无性能退化

3. **渐进式上线**
   - 先在测试环境验证
   - 再在生产环境灰度发布

---

## 9. 后续阶段调整

### 9.1 Phase 3: Integration & API（调整为 3-5 天）

**原计划：**
- WalBuilder 扩展（1 天）
- WalManager API 扩展（2 天）
- Recovery 支持（2-3 天）
- 完整功能测试（2 天）

**调整后：**
- ~~WalBuilder 扩展~~（已在 Phase 2 完成）
- ~~WalManager API 扩展~~（已在 Phase 2 完成）
- Recovery 支持（1 天）
- 完整功能测试（2 天）
- 文档完善（2 天）

**原因：**
- Phase 2 已完成核心 API 扩展
- Phase 3 重点调整为恢复支持和文档

### 9.2 Phase 4: Optimization & Testing（调整为 1 周）

**原计划：**
- Lock-free 批次构建器（3-4 天）
- 自适应提交调优（3-4 天）
- 压力测试（3 天）
- 崩溃恢复测试（2 天）
- 性能基准（2 天）

**调整后：**
- 性能优化（根据 Phase 3 测试结果）
  - 可能包括 Lock-free 队列
  - 可能包括自适应调优
- 压力测试（2 天）
- 崩溃恢复测试（2 天）
- 性能基准（1 天）

**原因：**
- 架构简化后，优化点减少
- 重点放在测试和验证

### 9.3 Phase 5: Documentation（保持 1 周）

- API 文档
- 使用示例
- 最佳实践
- 架构文档更新
- 性能调优指南

---

## 10. 总结

Phase 2 采用方案B（统一协调器），核心优势：

1. **架构最简**
   - 单一协调器（CommitCoordinator）
   - 移除 WriteCoordinator
   - 代码量减少 30%

2. **性能自适应**
   - 单 writer 自动退化到直接写入
   - 多 writer 自动启用 Group Commit
   - 无性能损失

3. **用户体验佳**
   - 无需关心模式选择
   - API 简洁统一
   - 向后兼容

4. **工作量减少**
   - 原计划：1-2 周
   - 调整后：**3-5 天**
   - 减少 50%+

5. **维护成本低**
   - 单一代码路径
   - 减少测试复杂度
   - 易于理解和调试

**预计完成时间：** 3-5 天

---

**文档版本**: v2.0  
**最后更新**: 2026-03-30  
**作者**: Easy WAL Team