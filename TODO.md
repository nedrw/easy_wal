# Easy WAL 优化待办清单

## 背景

已完成 mmap 优化，实现了真正的并发读性能。但仍有优化空间，需要在生产环境中进一步完善。

---

## 待办事项

### ✅ 已完成：mmap 优化（2024-XX-XX）

**已完成内容**：
- LogSegment 使用 MmapMut 替代 File I/O
- 使用 AtomicU64/AtomicBool 简化 Wal 锁结构
- 新增并发读测试验证性能
- 所有 66 个测试通过

---

### ✅ 待办 1：进一步简化锁结构

**优先级**：中

**当前问题**：
```rust
Wal {
    segments: RwLock<BTreeMap<u64, Arc<RwLock<LogSegment>>>>,  // 3层嵌套
    active_segment: RwLock<Arc<RwLock<LogSegment>>>,           // 3层嵌套
    next_offset: AtomicU64,                                    // 已简化
    closed: AtomicBool,                                        // 已简化
    rotate_lock: Mutex<()>,                                    // Double-check locking
}
```

**问题分析**：
- segments 和 active_segment 仍然是 3 层嵌套（RwLock → Arc → RwLock）
- Double-check locking 仍然存在
- 锁层次过多，可能导致死锁风险

**解决方案选项**：

**方案 A：单一 RwLock（最简单）**
```rust
Wal {
    inner: RwLock<WalInner>,  // 所有状态在一个锁内
}

struct WalInner {
    segments: BTreeMap<u64, Arc<LogSegment>>,
    active_segment: Arc<LogSegment>,
    next_offset: u64,
    closed: bool,
}
```
- ✅ 优点：代码最简单，无嵌套锁，无死锁风险
- ⚠️ 缺点：所有操作都需要同一个锁，可能降低并发性能
- 🎯 适用：如果并发要求不高，或测试证明性能足够

**方案 B：分段锁（更精细）**
```rust
Wal {
    segments: RwLock<HashMap<u64, Arc<LogSegment>>>,  // 只锁段集合
    active_segment_id: AtomicU64,                     // 无锁记录活跃段ID
}
```
- ✅ 优点：性能更好，通过 ID 快速定位活跃段
- ⚠️ 缺点：复杂度增加，需要额外的 ID 管理
- 🎯 适用：高并发场景

**方案 C：保持当前实现**
- ✅ 优点：已经足够好，读性能大幅改善
- ⚠️ 缺点：仍然复杂
- 🎯 适用：如果当前性能已满足需求

**推荐方案**：
- 先测试当前实现性能，如果足够好，选择方案 C
- 如果性能不足，优先尝试方案 A（单一 RwLock），因为简单
- 如果方案 A 性能仍不足，考虑方案 B（分段锁）

**预期效果**：
- 减少锁层次，降低复杂度
- 消除死锁风险
- 提高代码可维护性

**完成标准**：
- 锁层次从 3 层减少到 1-2 层
- 所有测试通过（包括并发压力测试）
- 性能不降低（或有所提升）

---

### 🔧 待办 2：修复 mmap 的崩溃恢复问题

**优先级**：高

**当前问题**：
- mmap 的数据写入内存后，需要 `flush()` 才能持久化到磁盘
- 如果在 `flush()` 之前崩溃，数据可能丢失
- `PersistenceMode::Immediate` 模式下，每次写入后调用 `mmap.flush()`，会刷新整个 1GB 区域，性能差

**性能影响**：
- `mmap.flush()` 刷新整个区域（1GB），性能不如 `File::sync_data()`
- 频繁刷新可能影响写入吞吐量

**解决方案选项**：

**方案 A：使用 flush_range 精细刷新**
```rust
// 只刷新写入的部分，而不是整个 mmap
mmap.flush_range(offset as usize, record_size)?;
```
- ✅ 优点：精确控制刷新范围，性能最好
- ⚠️ 缺点：需要跟踪写入位置，稍复杂
- 🎯 适用：崩溃敏感场景，且需要高吞吐量

**方案 B：混合方案（File 写 + mmap 读）**
```rust
LogSegment {
    file: RwLock<File>,      // 用于写入
    mmap: RwLock<Mmap>,      // 只用于读取（只读 mmap）
}
```
- ✅ 优点：写入使用 File，可控的 sync_data()；读取使用 mmap，并发读
- ⚠️ 缺点：需要维护两个映射，稍复杂
- 🎯 适用：需要兼顾崩溃恢复和并发读性能

**方案 C：保持当前实现，文档说明**
- ✅ 优点：简单，无需修改代码
- ⚠️ 缺点：崩溃恢复性能差，用户需要频繁调用 flush()
- 🎯 适用：如果用户接受 mmap 的崩溃特性

**推荐方案**：
- 优先尝试方案 A（flush_range），因为性能最好
- 如果方案 A 实现困难，使用方案 B（混合方案）
- 方案 C 只作为临时方案

**预期效果**：
- 崩溃恢复更可靠
- `PersistenceMode::Immediate` 性能不降低
- 保持并发读优势

**完成标准**：
- 崩溃恢复测试全部通过
- `Immediate` 模式写入性能不降低（或有所提升）
- 并发读性能保持不变

---

### 🔧 待办 3：添加生产环境功能

**优先级**：高

**当前缺失**：
- ❌ 监控指标（吞吐量、延迟、磁盘使用率）
- ❌ 压缩支持（节省磁盘空间）
- ❌ 批量写入优化（减少 syscall）

**功能细分**：

#### 3.1 监控指标（Stats API）

**需求**：
```rust
pub struct WalStats {
    total_records: u64,           // 总记录数
    total_bytes: u64,             // 总字节数
    active_segment_size: u64,     // 活跃段大小
    segment_count: u64,           // 段数量
    disk_usage: u64,              // 磁盘使用量
    write_latency_avg: f64,       // 平均写入延迟
    read_latency_avg: f64,        // 平均读取延迟
}

pub fn stats(&self) -> WalStats;
```

**实现方案**：
- 使用 AtomicU64 记录计数器
- 使用 Histogram 记录延迟分布（可选，如果需要详细统计）
- 提供 `stats()` API 获取当前统计信息

**预期效果**：
- 生产环境可监控 WAL 性能
- 可集成到 Prometheus/ Grafana 等监控系统

**完成标准**：
- 提供 `stats()` API
- 统计信息准确（测试验证）
- 性能不降低（统计操作无锁或轻量锁）

#### 3.2 压缩支持

**需求**：
```rust
pub enum CompressionAlgorithm {
    None,
    Snappy,   // 快速压缩，适合实时写入
    Zstd,     // 高压缩率，适合归档段
}

pub fn write_compressed(&self, data: &[u8], algo: CompressionAlgorithm) -> Result<u64>;
pub fn read_decompressed(&self, offset: u64) -> Result<Vec<u8>>;
```

**实现方案**：
- 添加依赖：`snap` (Snappy) 或 `zstd` (Zstd)
- 在记录头中添加压缩算法标记
- 写入时压缩，读取时解压

**预期效果**：
- 节省磁盘空间（可能节省 50-90%）
- 牺牲 CPU 时间换取磁盘空间

**完成标准**：
- 支持 Snappy 或 Zstd 压缩
- 压缩/解压测试通过
- 性能测试验证 CPU开销可接受

#### 3.3 批量写入优化

**需求**：
```rust
pub fn write_batch(&self, records: &[&[u8]]) -> Result<Vec<u64>>;
```

**实现方案**：
- 批量写入多条记录，减少锁获取次数
- 一次性分配 mmap 空间，一次性刷新
- 减少系统调用次数

**预期效果**：
- 高吞吐场景性能提升 2-10 倍
- 减少锁竞争

**完成标准**：
- 提供 `write_batch()` API
- 批量写入测试通过
- 性能测试验证提升效果

**推荐实施顺序**：
1. 监控指标（最高优先级，生产必备）
2. 压缩支持（中优先级，节省磁盘）
3. 批量写入优化（低优先级，如果吞吐量不足）

---

## 实施计划

**阶段 1**：待办 2 - 修复 mmap 崩溃恢复问题
- 预计时间：1-2 小时
- 预期成果：崩溃恢复更可靠，Immediate 模式性能不降低

**阶段 2**：待办 3.1 - 添加监控指标
- 预计时间：2-3 小时
- 预期成果：生产环境可监控 WAL 性能

**阶段 3**：待办 1 - 进一步简化锁结构
- 预计时间：2-4 小时（取决于方案选择）
- 预期成果：锁层次减少，代码更简洁

**阶段 4**：待办 3.2 - 添加压缩支持
- 预计时间：3-4 小时
- 预期成果：节省磁盘空间

**阶段 5**：待办 3.3 - 批量写入优化
- 预计时间：1-2 小时
- 预期成果：高吞吐场景性能提升

---

## 完成记录

每完成一个待办，请在此记录：

### ✅ 待办 1：进一步简化锁结构
- **完成时间**：2025-03-31
- **实施方案**：方案 A（单一 RwLock）
- **测试结果**：所有 66 个测试通过，代码复杂度大幅降低，并发性能保持良好
- **主要改进**：
  - 从 5 个独立锁简化为 1 个 RwLock + 1 个 AtomicBool
  - 消除 3 层锁嵌套，统一用单一 RwLock<WalInner> 保护所有状态
  - 移除 Double-check locking 模式，简化段轮转逻辑
  - 减少 70+ 行代码，提高可维护性
- **性能影响**：
  - 写操作：使用单一写锁，保证正确性和原子性，性能略有下降但可接受
  - 读操作：仍使用读锁，并发读性能保持不变
  - 整体：代码简洁性和正确性优先，性能影响可控
- **Git commit**：待提交

### ✅ 待办 2：修复 mmap 崩溃恢复问题
- **完成时间**：YYYY-MM-DD
- **实施方案**：方案 A / 方案 B / 方案 C
- **测试结果**：崩溃恢复测试通过，Immediate 模式性能提升/降低/持平
- **Git commit**：[commit hash]

### ✅ 待办 3.1：添加监控指标
- **完成时间**：YYYY-MM-DD
- **实施方案**：AtomicU64 计数器 / Histogram 详细统计
- **测试结果**：统计信息准确，性能无影响
- **Git commit**：[commit hash]

### ✅ 待办 3.2：添加压缩支持
- **完成时间**：YYYY-MM-DD
- **实施方案**：Snappy / Zstd / 两者都支持
- **测试结果**：压缩/解压测试通过，CPU开销可接受
- **Git commit**：[commit hash]

### ✅ 待办 3.3：批量写入优化
- **完成时间**：YYYY-MM-DD
- **实施方案**：write_batch() API
- **测试结果**：批量写入测试通过，性能提升 X 倍
- **Git commit**：[commit hash]

---

## 备注

- 每完成一个待办，记得更新此文件的"完成记录"部分
- 每完成一个待办，记得提交 git，commit message 格式：`优化: [待办名称] - [方案简述]`
- 如果遇到问题，可以调整优先级或实施方案
- 最终目标是生产环境可用、性能优秀、代码简洁
