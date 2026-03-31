# Easy WAL 架构重构计划：Kafka 模式

## 文档信息

- **创建日期**: 2025-03-27
- **决策者**: 项目负责人
- **状态**: 设计阶段
- **优先级**: P2（长期优化）

---

## 1. 问题背景

### 1.1 当前架构问题

#### 问题 #4：段管理器实例不共享

**症状**：
```
WriteCoordinator -> SegmentCoordinator -> SegmentManager (实例 #1)
ReadCoordinator -> LogReader -> SegmentManager (实例 #2)
```

**影响**：
- 两个 SegmentManager 实例状态不同步
- WriteCoordinator 创建新段时，LogReader 不知道
- 可能导致读取失败或数据不一致

#### 临时修复方案（当前）

**方案 B 简化版**：LogReader 通过文件系统检查段存在性

```rust
fn segment_exists(&self, segment_id: u64) -> bool {
    let path = self.config.dir.join(format!("segment_{}.log", segment_id));
    path.exists()
}
```

**优点**：
- 解决了状态同步问题
- 实现简单，修改量小

**缺点**：
- 不符合架构原则：LogReader 仍然有段管理逻辑
- 绕过了状态管理，依赖文件系统
- 不适合需要段统计、清理等功能的场景
- 与"协调层决策，组件层执行"原则冲突

---

### 1.2 架构原则回顾

根据 `docs/analysis-segment-management.md` 的分析：

**核心原则**：
1. **底层应该纯粹**：Storage 层只负责字节读写，组件层只负责特定功能
2. **高层应该决策**：协调层负责策略、决策、协调
3. **段切换是决策**：应该由协调层决策，而不是组件层

**当前违反原则**：
- LogReader（组件层）决策段切换
- LogReader 检查段存在性（段管理逻辑）

---

## 2. 业界方案调研

### 2.1 三种主流模式

| 模式 | 代表库 | 段管理位置 | 是否共享状态 | 适用场景 |
|------|--------|-----------|-------------|----------|
| **Writer-Reader 分离** | etcd, RocksDB | Reader 内部 | 否 | 简单场景，单文件 WAL |
| **统一 Log 对象** | Kafka, SQLite | Log 对象内部 | 是 | 复杂场景，分段 WAL |
| **混合模式** | 当前 Easy WAL | 分散 | 部分 | ❌ 架构不清晰 |

### 2.2 etcd/raft WAL 模式（分离模式）

```go
// Go - etcd 的 WAL 架构
type WAL struct {
    dir      string
    metadata []byte
    encoder  *encoder
    // 注意：没有 Reader！
}

// 读取是独立函数，不是对象
func ReadAll(rc io.Reader) ([]byte, *raftpb.HardState, []byte, error) {
    // 直接打开文件，不依赖任何状态
    // 内部判断段切换
}
```

**优点**：
- Writer 和 Reader 完全解耦
- 不共享状态，实现简单

**缺点**：
- Reader 函数需要处理所有细节
- 用户需要自己管理读取位置
- 不适合复杂场景（如预读缓冲、并发读取）

### 2.3 Kafka Log 模式（统一模式）

#### 2.3.1 Kafka 的三层架构

```java
// Java - Kafka 的日志架构（三层）

// Layer 1: Log - 统一的日志对象，管理所有段
class Log {
    LogSegments segments;  // 段集合
    Leases leases;         // 租约管理
    
    // 读写都通过 Log 对象
    long append(...) { ... }
    FetchDataInfo read(...) { ... }
}

// Layer 2: LogSegment - 单个段，包含读写能力（关键！）
class LogSegment {
    private final FileRecords records;  // 文件存储
    private final long baseOffset;      // 起始偏移量
    
    // 注意：LogSegment 同时提供读写能力！
    void append(...) { ... }           // 写入
    FetchDataInfo read(...) { ... }     // 读取
}

// Layer 3: FileRecords - 文件记录，底层存储
class FileRecords {
    private final File file;
    private final FileChannel channel;
    
    // 底层的文件读写
    int write(...) { ... }
    Records read(...) { ... }
}
```

**关键发现**：
- **Kafka 没有 LogWriter 和 LogReader！**
- **LogSegment 同时负责读写**，是一个内聚的组件
- FileRecords 是纯粹的文件存储抽象

#### 2.3.2 Kafka 的数据流

```
写入流程：
Log.append(records)
    ↓
Log.selectSegment()  // 选择段
    ↓
LogSegment.append(records)  // 段负责写入
    ↓
FileRecords.write(records)  // 文件存储

读取流程：
Log.read(offset, length)
    ↓
Log.locateSegment(offset)  // 定位段
    ↓
LogSegment.read(offset, length)  // 段负责读取
    ↓
FileRecords.read(offset, length)  // 文件存储
```

**核心设计原则**：
1. **段是读写的基本单元**：LogSegment 同时负责读写
2. **状态内聚**：读写共享同一个文件句柄和状态
3. **分层清晰**：Log（协调层）→ LogSegment（组件层）→ FileRecords（存储层）

**优点**：
- 状态一致（读写共享段状态）
- 内聚性强（段的读写逻辑集中）
- 架构简洁（减少组件数量）
- 用户接口简单（一个对象搞定读写）

**缺点**：
- LogSegment 职责稍重
- 需要仔细设计并发控制

---

## 3. Kafka 模式架构设计

### 3.1 核心设计原则

1. **段切换逻辑上移到协调层**：ReadCoordinator 负责段切换决策
2. **LogReader 职责纯粹化**：只负责段内读取，不决策段切换
3. **状态共享**：ReadCoordinator 和 WriteCoordinator 共享 SegmentCoordinator
4. **协调层决策，组件层执行**：符合架构分层原则

---

### 3.2 架构对比

#### 当前架构（问题架构）

```
┌─────────────────────────────────────────┐
│         Layer 4: API层                    │
│  WalManager                               │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│         Layer 3: 协调层                   │
│  WriteCoordinator   ReadCoordinator      │
│         ↓                  ↓             │
│  SegmentCoordinator  LogReader           │
│         ↓                  ↓             │
│  SegmentManager #1   SegmentManager #2   │ ← 问题：状态不同步
└─────────────────────────────────────────┘
```

**问题**：
1. LogWriter 和 LogReader 分离，导致状态分散
2. 两个独立的 SegmentManager，状态不同步
3. 组件数量多，架构复杂

#### 真正的 Kafka 模式架构（推荐）

```
┌─────────────────────────────────────────┐
│         Layer 4: API层                    │
│  WalManager                               │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│         Layer 3: 协调层                   │
│  ┌──────────────────────────────────┐   │
│  │   SegmentCoordinator（共享）      │   │
│  │   - 段生命周期管理                │   │
│  │   - 段选择和定位                  │   │
│  │   - 段切换决策                    │   │
│  └──────────────────────────────────┘   │
│         ↓                ↓               │
│  WriteCoordinator   ReadCoordinator      │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│         Layer 2: 组件层                   │
│  ┌──────────────────────────────────┐   │
│  │   LogSegment（新增，整合）        │   │
│  │   - 段内写入（原 LogWriter）      │   │
│  │   - 段内读取（原 LogReader）      │   │
│  │   - 状态内聚，读写共享            │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘
                  ↓
┌─────────────────────────────────────────┐
│         Layer 1: Storage层               │
│  SegmentManager     FileStorage          │
│  - 段文件管理       - 文件字节读写        │
└─────────────────────────────────────────┘
```

**关键改进**：
1. **整合 LogWriter 和 LogReader 为 LogSegment**
2. **状态内聚**：读写共享同一个文件句柄和状态
3. **减少组件**：从 2 个组件（LogWriter + LogReader）简化为 1 个（LogSegment）
4. **对标 Kafka**：完全符合 Kafka 的 LogSegment 设计

---

### 3.3 关键组件职责重定义

#### 3.3.1 新组件：LogSegment（整合 LogWriter + LogReader）

**设计理念**：对标 Kafka 的 LogSegment，将读写能力整合到一个组件中。

**职责**：
- ✅ **段内写入**（原 LogWriter 职责）
- ✅ **段内读取**（原 LogReader 职责）
- ✅ **状态内聚**：读写共享同一个文件句柄和状态
- ✅ **纯粹的组件层**：不决策段切换，只负责段内读写

**接口设计**：
```rust
/// 日志段 - 整合读写能力
/// 
/// 对标 Kafka 的 LogSegment 设计：
/// - 同时提供读写能力
/// - 状态内聚，读写共享文件句柄
/// - 不决策段切换，只负责段内操作
pub struct LogSegment {
    /// 段 ID
    segment_id: u64,
    /// 文件存储
    storage: Arc<FileStorage>,
    /// 段配置
    config: SegmentConfig,
    /// 当前写入位置（读写共享）
    write_position: RwLock<u64>,
}

impl LogSegment {
    /// 创建日志段
    pub async fn new(segment_id: u64, path: &Path) -> Result<Self> {
        let storage = Arc::new(FileStorage::new(path).await?);
        
        Ok(Self {
            segment_id,
            storage,
            config: SegmentConfig::default(),
            write_position: RwLock::new(0),
        })
    }
    
    // ========== 写入能力（原 LogWriter）==========
    
    /// 追加数据到段末尾
    pub async fn append(&self, data: &[u8]) -> Result<(u64, u64)> {
        let offset = *self.write_position.read().await;
        self.storage.write(offset, data).await?;
        *self.write_position.write().await = offset + data.len() as u64;
        Ok((self.segment_id, offset))
    }
    
    /// 刷盘
    pub async fn flush(&self) -> Result<()> {
        self.storage.flush().await
    }
    
    /// 获取当前写入位置
    pub async fn write_position(&self) -> u64 {
        *self.write_position.read().await
    }
    
    // ========== 读取能力（原 LogReader）==========
    
    /// 从段内指定位置读取数据
    pub async fn read(&self, offset: u64, length: usize) -> Result<Vec<u8>> {
        self.storage.read(offset, length).await
    }
    
    /// 批量读取（预读优化）
    pub async fn read_batch(&self, offset: u64, max_length: usize) -> Result<Vec<u8>> {
        self.storage.read_batch(offset, max_length).await
    }
    
    /// 获取段大小
    pub async fn size(&self) -> u64 {
        *self.write_position.read().await
    }
}
```

**优势**：
1. **内聚性强**：段的读写逻辑集中在一个地方
2. **状态一致**：读写共享同一个 write_position，避免状态不同步
3. **架构简洁**：减少组件数量，降低复杂度
4. **对标 Kafka**：完全符合 Kafka 的 LogSegment 设计

#### 3.3.2 ReadCoordinator（使用 LogSegment）

**职责**：
- ✅ 持有共享的 SegmentCoordinator
- ✅ **负责段切换决策**
- ✅ 管理预读缓冲区
- ✅ 管理读取位置
- ✅ 调用 LogSegment 进行段内读取

**方法实现**：
```rust
impl ReadCoordinator {
    /// 读取数据（包含段切换逻辑）
    pub async fn read(&self, length: usize) -> Result<Vec<u8>> {
        let mut result = Vec::new();
        let mut remaining = length;
        let (mut segment_id, mut offset) = self.position.get().await;
        
        while remaining > 0 {
            // 1. 获取当前段（通过 SegmentCoordinator）
            let segment = self.segment_coordinator.get_segment(segment_id).await?;
            
            // 2. 从段读取数据
            let data = segment.read(offset, remaining).await?;
            
            if data.len() == 0 {  // 当前段读完
                // 3. ReadCoordinator 决策：是否有下一个段
                let next_segment_id = segment_id + 1;
                if self.segment_coordinator.has_segment(next_segment_id).await {
                    // 4. ReadCoordinator 决策：切换段
                    segment_id = next_segment_id;
                    offset = SEGMENT_HEADER_SIZE;
                    self.position.update(segment_id, offset).await;
                    continue;
                } else {
                    // 没有下一个段，返回已读取的数据
                    return Ok(result);
                }
            }
            
            result.extend(&data);
            remaining -= data.len();
            offset += data.len() as u64;
            self.position.update(segment_id, offset).await;
        }
        
        Ok(result)
    }
}
```

#### 3.3.3 WriteCoordinator（使用 LogSegment）

**职责**：
- ✅ 持有共享的 SegmentCoordinator
- ✅ 管理写入缓冲
- ✅ 协调段轮转
- ✅ 调用 LogSegment 进行段内写入

**方法实现**：
```rust
impl WriteCoordinator {
    /// 写入数据
    pub async fn write(&self, data: &[u8]) -> Result<(u64, u64)> {
        // 1. 获取活跃段（通过 SegmentCoordinator）
        let segment = self.segment_coordinator.get_active_segment().await?;
        
        // 2. 检查是否需要轮转
        if segment.size().await + data.len() as u64 > self.max_segment_size {
            // 3. 协调段轮转（决策层）
            self.segment_coordinator.rotate().await?;
            let segment = self.segment_coordinator.get_active_segment().await?;
        }
        
        // 4. 写入数据到段（执行层）
        let pos = segment.append(data).await?;
        
        Ok(pos)
    }
}
```

#### 3.3.4 SegmentCoordinator（增强）

**当前职责**：
- 段生命周期管理
- 段轮转策略决策
- 段状态查询

**重构后职责**：
- ✅ 段生命周期管理
- ✅ 段轮转策略决策
- ✅ 段状态查询
- ✅ **提供段存在性查询接口**（已实现 `get_segment()`）
- ✅ **提供段路径查询接口**（已实现 `segment_path()`）

---

### 3.4 数据流对比

#### 当前数据流（问题流）

```
用户调用 wal.read(100)
    ↓
ReadCoordinator.read(100)
    ↓
LogReader.read_raw(100)
    ↓
LogReader 内部循环：
    - 从当前段读取
    - 检查段存在性（通过 SegmentManager）
    - 决策是否切换段
    - 执行段切换
    ↓
返回数据
```

**问题**：
- LogReader 内部有段管理逻辑
- 依赖独立的 SegmentManager，状态可能不同步

#### 重构后数据流（清晰流）

```
用户调用 wal.read(100)
    ↓
ReadCoordinator.read(100)
    ↓
ReadCoordinator 内部循环：
    - 调用 LogReader.read_from_segment(segment_id, offset, remaining)
    - 检查段存在性（通过共享的 SegmentCoordinator）
    - 决策是否切换段
    - 执行段切换
    ↓
返回数据
```

**优点**：
- ReadCoordinator 负责段切换决策（协调层决策）
- LogReader 只负责段内读取（组件层执行）
- 状态一致（共享 SegmentCoordinator）

---

## 4. 重构实施计划

### 4.1 分阶段实施

#### Phase 1: LogReader 职责纯粹化（3-5 天）

**目标**：移除 LogReader 的段切换逻辑，暴露纯粹的段内读取接口

**任务清单**：
- [ ] 移除 `read_raw()` 方法中的段切换逻辑
- [ ] 移除 `read_raw_at()` 方法中的段切换逻辑
- [ ] 移除 `read_next()` 方法中的段切换逻辑
- [ ] 添加 `read_from_segment()` 纯粹接口
- [ ] 添加 `read_batch_from_segment()` 纯粹接口
- [ ] 移除 `segment_manager` 字段
- [ ] 更新单元测试

**验证**：
- LogReader 只提供段内读取功能
- 所有段切换逻辑已移除
- 单元测试通过

---

#### Phase 2: ReadCoordinator 段切换逻辑（5-7 天）

**目标**：将段切换逻辑上移到 ReadCoordinator

**任务清单**：
- [ ] ReadCoordinator 添加 `segment_coordinator: Arc<SegmentCoordinator>` 字段
- [ ] ReadCoordinator 添加 `position: ReadCoordPosition` 字段（已存在）
- [ ] 实现 `read()` 方法的段切换逻辑
- [ ] 实现 `read_raw_at()` 方法的段切换逻辑
- [ ] 实现预读缓冲区的段切换处理
- [ ] 更新 `seek()` 方法
- [ ] 更新 `position()` 方法
- [ ] 更新集成测试

**验证**：
- ReadCoordinator 负责段切换决策
- LogReader 只负责段内读取
- 所有测试通过

---

#### Phase 3: WalManager 创建流程调整（2-3 天）

**目标**：修改 WalManager::new()，让 ReadCoordinator 使用共享的 SegmentCoordinator

**任务清单**：
- [ ] 修改 `WalManager::new()` 的创建流程
- [ ] ReadCoordinator 使用共享的 SegmentCoordinator
- [ ] 移除 LogReader 的独立 SegmentManager 创建
- [ ] 更新配置和构造方法
- [ ] 更新文档和示例

**验证**：
- WriteCoordinator 和 ReadCoordinator 共享 SegmentCoordinator
- 所有测试通过

---

#### Phase 4: 测试和验证（3-5 天）

**目标**：全面测试重构后的架构

**任务清单**：
- [ ] 运行所有单元测试
- [ ] 运行所有集成测试
- [ ] 运行崩溃恢复测试
- [ ] 运行压力测试
- [ ] 性能基准测试
- [ ] 代码审查

**验证**：
- 所有测试通过
- 性能没有退化
- 代码质量符合标准

---

### 4.2 风险评估

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|----------|
| 破坏现有功能 | 高 | 中 | 分阶段实施，充分测试 |
| 性能退化 | 中 | 低 | 性能基准测试，优化热点 |
| 接口不兼容 | 高 | 低 | 保持公共接口稳定 |
| 时间延期 | 中 | 中 | 预留 buffer 时间 |

---

### 4.3 回滚策略

**如果重构失败**：
1. 回退到当前修复方案（文件系统检查）
2. 保留 Phase 1 的代码（LogReader 职责纯粹化）
3. 延迟实施，等待更好的时机

---

## 5. 预期收益

### 5.1 架构收益

1. **职责清晰**：
   - ReadCoordinator：段切换决策
   - LogReader：段内读取执行
   - 符合"协调层决策，组件层执行"原则

2. **状态一致**：
   - 读写共享 SegmentCoordinator
   - 消除状态不同步问题

3. **易于扩展**：
   - 新增段管理功能（统计、清理）只需修改 SegmentCoordinator
   - 不影响 LogReader

4. **易于测试**：
   - LogReader 职责纯粹，易于单元测试
   - ReadCoordinator 的段切换逻辑可以独立测试

---

### 5.2 性能收益

1. **减少资源占用**：
   - 只有一个 SegmentManager 实例
   - 减少内存占用

2. **提高缓存效率**：
   - 共享的 SegmentCoordinator 缓存段元数据
   - 减少文件系统访问

---

### 5.3 维护性收益

1. **代码更清晰**：
   - 职责单一，易于理解
   - 符合架构原则

2. **易于调试**：
   - 状态集中管理
   - 问题定位更容易

---

## 6. 决策记录

### 6.1 为什么选择 Kafka 模式？

1. **符合架构原则**：
   - 协调层决策，组件层执行
   - 段切换是决策，应该在协调层

2. **状态一致**：
   - 读写共享段状态，消除了同步问题

3. **业界验证**：
   - Kafka 是生产级系统，久经考验
   - 架构清晰，易于维护

4. **适合 Easy WAL 的场景**：
   - 分段 WAL
   - 需要段管理功能
   - 需要预读缓冲

---

### 6.2 为什么不选择 etcd 模式？

1. **不适合分段场景**：
   - etcd 的 Reader 是函数，不适合管理复杂状态
   - Easy WAL 有预读缓冲，需要对象管理状态

2. **用户体验差**：
   - 用户需要自己处理段切换
   - 不符合 Easy WAL 的易用性目标

---

### 6.3 为什么不保持当前方案？

1. **违反架构原则**：
   - LogReader 有段管理逻辑
   - 不符合"协调层决策，组件层执行"

2. **绕过状态管理**：
   - 依赖文件系统检查，而不是状态管理
   - 不适合未来的段管理功能

3. **不是根本解决方案**：
   - 只是绕过了问题，没有解决问题

---

## 7. 后续优化事项

### 7.1 短期优化（随重构一起实施）

- [ ] 优化预读缓冲区的段切换处理
- [ ] 添加段切换的性能监控
- [ ] 改进段存在性检查的缓存策略

### 7.2 长期优化（重构后实施）

- [ ] 段统计功能（通过 SegmentCoordinator）
- [ ] 段清理功能（通过 SegmentCoordinator）
- [ ] 段压缩功能
- [ ] 更智能的预读策略

---

## 8. 参考资料

### 8.1 相关文档

- `docs/analysis-segment-management.md`：段管理架构优化分析
- `docs/known-issues.md`：已知问题列表

### 8.2 业界参考

- **Kafka Log 设计**：分段日志的典范
- **etcd WAL 设计**：Writer-Reader 分离模式
- **RocksDB WAL 设计**：单文件 WAL 模式

---

## 9. 变更记录

| 日期 | 版本 | 变更内容 | 作者 |
|------|------|----------|------|
| 2025-03-27 | v1.0 | 创建文档，记录 Kafka 模式重构方案 | AI Assistant |
| 2025-03-27 | v1.1 | **重大更新：整合 LogWriter 和 LogReader 为 LogSegment，完全对标 Kafka 架构** | AI Assistant |

---

## 10. 待办事项

- [ ] 评审此设计文档
- [ ] 确认实施优先级和时间表
- [ ] 分配开发资源
- [ ] 创建详细的实施任务清单
- [ ] 开始 Phase 1 实施
