# Multi-Writer WAL Extension Design

## 1. Overview

This document outlines the design for extending the current single-writer WAL implementation to support **multiple concurrent writers** using a **RocksDB-style Group Commit** approach.

**Current State (Single-Writer):**
- `WalManager` holds one `WriteCoordinator` and one `ReadCoordinator`
- `WriteCoordinator` holds a single `Arc<LogWriter>`
- All writes are serialized through the `LogWriter`

**Target State (Multi-Writer with Group Commit):**
- Multiple concurrent writers can submit writes in parallel
- Writes are batched and committed together (Group Commit)
- Global ordering maintained via sequence numbers
- Readers can read concurrently (already supported)

---

## 2. RocksDB-Style Group Commit Architecture

### 2.1 Core Design

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Multi-Writer WAL                              │
│                                                                      │
│   Writer1 ─────┐                                                     │
│   Writer2 ─────┼──► WriteBatchBuilder ──► Concurrent Build          │
│   Writer3 ─────┘         (lock-free)                                 │
│                           │                                          │
│                           ▼                                          │
│   ┌─────────────────────────────────────────────────────────────┐    │
│   │                   Commit Coordinator                         │    │
│   │                                                              │    │
│   │   ┌──────────────────────────────────────────────────────┐  │    │
│   │   │  Commit Queue: [Batch1, Batch2, Batch3, ...]          │  │    │
│   │   │                    │                                   │  │    │
│   │   │                    ▼                                   │  │    │
│   │   │  ┌─────────────────────────────────────────────────┐  │  │    │
│   │   │  │  Group Commit Loop (single thread)              │  │  │    │
│   │   │  │    1. Collect batches until timeout or size    │  │  │    │
│   │   │  │    2. Merge batches into single write buffer    │  │  │    │
│   │   │  │    3. Single fsync for all batches              │  │  │    │
│   │   │  │    4. Notify all writers with positions         │  │  │    │
│   │   │  └─────────────────────────────────────────────────┘  │  │    │
│   │   └──────────────────────────────────────────────────────┘  │    │
│   └──────────────────────────────────────────────────────────────┘    │
│                              │                                        │
│                              ▼                                        │
│                    ┌─────────────────────┐                            │
│                    │    LogWriter        │                            │
│                    │  (Segment + Sync)   │                            │
│                    └─────────────────────┘                            │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Key Differences from Original Single-Writer

| Aspect | Single-Writer | Group Commit |
|--------|---------------|--------------|
| Write Latency | I/O bound (waits for fsync) | Lower (batched I/O) |
| Throughput | Limited by fsync frequency | Improved via batch grouping |
| Lock Contention | High (all writes serialized) | Low (build phase lock-free) |
| Implementation | Simple | Moderate complexity |

---

## 3. Component Design

### 3.1 WriteBatch

```rust
/// A batch of writes from a single writer
pub struct WriteBatch {
    /// Unique batch identifier
    pub batch_id: u64,
    /// Writer's unique identifier
    pub writer_id: u64,
    /// All records in this batch
    pub records: Vec<Vec<u8>>,
    /// Total size in bytes
    pub size_bytes: usize,
    /// Sequence number assigned by coordinator
    pub sequence: AtomicU64,
    /// Channel to send result back to writer
    pub result_tx: oneshot::Sender<Result<Vec<WritePosition>>>,
    /// Timestamp for timeout tracking
    pub created_at: std::time::Instant,
}

impl WriteBatch {
    pub fn new(writer_id: u64, records: Vec<Vec<u8>>) -> (Self, oneshot::Receiver<Result<Vec<WritePosition>>>) {
        let (tx, rx) = oneshot::channel();
        let size_bytes = records.iter().map(|r| r.len()).sum();
        
        (Self {
            batch_id: 0, // Assigned by coordinator
            writer_id,
            records,
            size_bytes,
            sequence: AtomicU64::new(0),
            result_tx: tx,
            created_at: std::time::Instant::now(),
        }, rx)
    }
}
```

### 3.2 CommitCoordinator

```rust
/// Coordinates group commit of multiple batches
pub struct CommitCoordinator {
    /// Configuration
    config: CommitConfig,
    
    /// Global sequence number (atomic)
    next_sequence: AtomicU64,
    
    /// Commit queue - batches waiting to be committed
    pending_batches: Mutex<Vec<Arc<WriteBatch>>>,
    
    /// Conditional variable for commit loop
    commit_condvar: Condvar,
    
    /// Commit loop task handle
    commit_task: Arc<Mutex<Option<JoinHandle<()>>>>,
    
    /// Statistics
    stats: CommitStats,
}

/// Configuration for group commit
#[derive(Debug, Clone)]
pub struct CommitConfig {
    /// Maximum batch size before forcing commit (bytes)
    pub max_batch_size: usize,
    /// Maximum wait time before forcing commit (ms)
    pub max_wait_time_ms: u64,
    /// Maximum batches to collect before commit
    pub max_batch_count: usize,
    /// Minimum batches to trigger commit (if 0, single batch commits immediately)
    pub min_batches_for_commit: usize,
}

impl Default for CommitConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 64 * 1024,      // 64KB
            max_wait_time_ms: 5,             // 5ms
            max_batch_count: 100,            // 100 batches
            min_batches_for_commit: 1,       // Can commit with single batch
        }
    }
}
```

### 3.3 MultiWriterCoordinator

```rust
/// Multi-writer coordinator - main entry point for writers
pub struct MultiWriterCoordinator {
    /// Commit coordinator
    commit_coordinator: Arc<CommitCoordinator>,
    
    /// Writer registry for tracking
    writers: RwLock<HashMap<u64, WriterMeta>>,
    
    /// Next available writer ID
    next_writer_id: AtomicU64,
    
    /// Reference to underlying LogWriter
    log_writer: Arc<LogWriter>,
}

/// Writer metadata
struct WriterMeta {
    writer_id: u64,
    batches_submitted: AtomicU64,
    records_written: AtomicU64,
    bytes_written: AtomicU64,
}
```

---

## 4. Write Flow

### 4.1 Writer Submit Path (Lock-Free)

```
Writer Thread                          Coordinator Thread
     │                                        │
     │  1. Build WriteBatch locally           │
     │  (no locks needed)                     │
     │                                        │
     │  2. Acquire commit_queue lock briefly  │
     │  ─────────────────────────────────────►│
     │  3. Push batch to pending_batches      │
     │  4. Release lock                       │
     │◄─────────────────────────────────────  │
     │                                        │
     │  5. Return future to writer            │
     │     (will be fulfilled later)          │
     ▼                                        ▼
```

### 4.2 Commit Loop (Single Thread)

```rust
impl CommitCoordinator {
    /// Main commit loop - runs in dedicated task
    async fn commit_loop(self: Arc<Self>) {
        let mut timer = Interval::at(
            std::time::Duration::from_millis(self.config.max_wait_time_ms)
        );
        
        loop {
            tokio::select! {
                // Timeout-based trigger
                _ = timer.tick() => {
                    self.flush_pending_batches().await;
                }
                
                // Shutdown signal
                _ = self.shutdown_rx.changed() => {
                    if *self.shutdown_rx.borrow() {
                        // Final flush before shutdown
                        self.flush_pending_batches().await;
                        break;
                    }
                }
            }
        }
    }
    
    /// Collect and commit pending batches
    async fn flush_pending_batches(&self) {
        // 1. Collect batches
        let batches = {
            let mut pending = self.pending_batches.lock().unwrap();
            if pending.is_empty() {
                return;
            }
            std::mem::take(&mut *pending)
        };
        
        // 2. Assign sequence numbers
        let mut sequence = self.next_sequence.fetch_add(
            batches.len() as u64,
            Ordering::SeqCst
        );
        
        for batch in &batches {
            batch.sequence.store(sequence, Ordering::SeqCst);
            sequence += 1;
        }
        
        // 3. Merge records into single buffer
        let merged = self.merge_batches(&batches);
        
        // 4. Single write to LogWriter
        let positions = self.log_writer.write_batch(&merged).await;
        
        // 5. Fsync (once for all batches)
        self.log_writer.sync().await;
        
        // 6. Notify all writers
        for (i, batch) in batches.iter().enumerate() {
            let start_pos = i * batch.records.len();
            let end_pos = start_pos + batch.records.len();
            let batch_positions = &positions[start_pos..end_pos];
            
            let _ = batch.result_tx.send(Ok(batch_positions.to_vec()));
        }
        
        // 7. Update stats
        self.stats.record_commit(batches.len());
    }
    
    /// Merge multiple batches into ordered record list
    fn merge_batches(&self, batches: &[Arc<WriteBatch>]) -> Vec<Vec<u8>> {
        let mut result = Vec::with_capacity(
            batches.iter().map(|b| b.records.len()).sum()
        );
        
        // Sort by sequence number to maintain order
        let mut sorted = batches.to_vec();
        sorted.sort_by_key(|b| b.sequence.load(Ordering::SeqCst));
        
        for batch in sorted {
            for record in &batch.records {
                result.push(record.clone());
            }
        }
        
        result
    }
}
```

---

## 5. Sequence Number Management

### 5.1 Global Ordering

```rust
/// Each record gets a globally unique, monotonically increasing sequence number
/// This ensures all readers see writes in the exact same order

pub struct SequenceNumber {
    /// High 32 bits: commit group ID
    commit_group: u64,
    /// Low 32 bits: sequence within commit group  
    sequence: u64,
}

impl SequenceNumber {
    pub fn new(commit_group: u64, sequence: u64) -> Self {
        Self { commit_group, sequence }
    }
    
    /// Compare sequence numbers for ordering
    pub fn cmp(&self, other: &Self) -> Ordering {
        self.commit_group.cmp(&other.commit_group)
            .then_with(|| self.sequence.cmp(&other.sequence))
    }
}
```

### 5.2 Reader Synchronization

```rust
/// Reader must wait for all committed writes before reading
pub struct ReadGuard {
    /// Current committed sequence
    committed_sequence: u64,
    /// Latest readable position in WAL
    readable_position: WritePosition,
}

/// ReadCoordinator uses committed_sequence to ensure consistency
impl ReadCoordinator {
    pub async fn wait_for_sequence(&self, sequence: u64) {
        let committed = self.commit_coordinator.committed_sequence().await;
        if committed < sequence {
            // Wait until the sequence is committed
            self.commitNotifier.wait(sequence).await;
        }
    }
}
```

---

## 6. Crash Recovery

### 6.1 Recovery Protocol

```
1. On startup, scan all segments
         │
         ▼
2. Find last valid record with valid magic + CRC
         │
         ▼
3. Extract sequence number from last valid record
         │
         ▼
4. Set committed_sequence = last_valid_sequence
         │
         ▼
5. Readers start from last valid record position
         │
         ▼
6. Pending (uncommitted) writes are lost - acceptable
   because they were not fsynced before crash
```

### 6.2 Partial Write Handling

Because Group Commit batches multiple writes:

- **Scenario**: System crashes during `fsync`
- **Result**: Entire group commit is lost (not partially written)
- **Mitigation**: 
  - Record boundaries use magic + length + CRC
  - No torn writes possible (kernel handles atomicity)

### 6.3 Write-Ahead Guarantee

Each `write()` to the coordinator returns only AFTER:
1. Record is in memory buffer (queue)
2. fsync has completed

This guarantees durability of all returned writes.

---

## 7. Configuration

### 7.1 Commit Configuration

```rust
/// Group commit configuration
#[derive(Debug, Clone)]
pub struct CommitConfig {
    /// Maximum bytes to collect before forcing commit
    /// Default: 64KB
    pub max_batch_size: usize,
    
    /// Maximum time to wait before forcing commit  
    /// Default: 5ms (tuned for NVMe SSDs)
    pub max_wait_time_ms: u64,
    
    /// Maximum number of batches to collect
    /// Default: 100
    pub max_batch_count: usize,
    
    /// Minimum batches to trigger commit (0 = immediate)
    /// Default: 1
    pub min_batches_for_commit: usize,
}

/// Example: Tuned for high-throughput NVMe
let config = CommitConfig {
    max_batch_size: 256 * 1024,  // 256KB
    max_wait_time_ms: 2,         // 2ms
    max_batch_count: 1000,
    min_batches_for_commit: 8,    // Wait for at least 8 batches
};
```

### 7.2 Performance Characteristics

| Config | Latency | Throughput | Use Case |
|--------|---------|------------|----------|
| Aggressive (2ms, 8 batches) | ~2-5ms | Highest | Batch processing |
| Balanced (5ms, 1 batch) | ~5-10ms | High | General purpose |
| Low Latency (0ms, 1 batch) | ~0.5-2ms | Medium | Real-time |

---

## 8. API Design

### 8.1 New Types

```rust
// In wal/multi_writer.rs

/// Multi-writer WAL entry point
pub struct MultiWriterWal {
    coordinator: Arc<MultiWriterCoordinator>,
    read_coordinator: Arc<ReadCoordinator>,
    recovery_manager: RecoveryManager,
    config: WalConfig,
}

/// Handle for a registered writer
pub struct WriterHandle {
    writer_id: u64,
    coordinator: Arc<MultiWriterCoordinator>,
}

/// Result of a batch write
pub struct BatchWriteResult {
    /// Positions of each record in the batch
    pub positions: Vec<WritePosition>,
    /// Sequence number assigned to first record
    pub start_sequence: u64,
    /// Commit timestamp (after fsync completes)
    pub committed_at: std::time::Instant,
}
```

### 8.2 WalBuilder Extensions

```rust
impl WalBuilder {
    /// Enable multi-writer mode with group commit
    pub fn with_multi_writer(mut self, config: CommitConfig) -> Self {
        self.config.multi_writer = Some(config);
        self
    }
    
    /// Use default group commit settings
    pub fn with_multi_writer_default(mut self) -> Self {
        self.config.multi_writer = Some(CommitConfig::default());
        self
    }
}

/// WalConfig extension
impl WalConfig {
    pub multi_writer: Option<CommitConfig>,
}
```

### 8.3 Usage Example

```rust
// Create multi-writer WAL
let wal = WalBuilder::new()
    .with_dir("/tmp/wal")
    .with_multi_writer_default()
    .build()
    .await?;

// Get writer handle
let writer = wal.writer_handle(1); // writer_id = 1

// Write batch (async, returns when committed)
let result = writer.write_batch(&[b"record1", b"record2"]).await?;
println!("Written at sequence {}", result.start_sequence);

// Multiple writers can write concurrently
let writer2 = wal.writer_handle(2);
let result2 = writer2.write_batch(&[b"record3"]).await?;

// Reader sees all committed writes in order
let pos = wal.position().await;
wal.seek_to_start().await;
while let Ok(record) = wal.read().await {
    // process record in sequence order
}
```

---

## 9. Comparison with Original Design

### 9.1 Changes to Existing Components

| Component | Original | New |
|-----------|----------|-----|
| `WriteCoordinator` | Single writer | Replaced by `MultiWriterCoordinator` |
| `LogWriter` | No changes | No changes |
| `LogReader` | No changes | No changes |
| `ReadCoordinator` | No changes | Minor: add sequence awareness |
| `WalManager` | Single writer API | Add multi-writer API |

### 9.2 Backward Compatibility

- **Single-writer mode**: `WalBuilder` without `with_multi_writer()` works exactly as before
- **Multi-writer mode**: New API, not backward compatible
- **WAL file format**: Unchanged (no modification to storage layer)

---

## 10. Implementation Phases

### Phase 1: Core Infrastructure
- [ ] Define `WriteBatch`, `CommitCoordinator`, `CommitConfig` types
- [ ] Implement sequence number allocation
- [ ] Implement commit loop with timer

### Phase 2: MultiWriterCoordinator
- [ ] Implement `MultiWriterCoordinator::submit_batch()`
- [ ] Implement batch collection and merging
- [ ] Implement result notification via oneshot

### Phase 3: Integration
- [ ] Extend `WalBuilder` with multi-writer options
- [ ] Implement `WalManager::writer_handle()`
- [ ] Add recovery support for multi-writer

### Phase 4: Optimization
- [ ] Lock-free batch builder (optional)
- [ ] Adaptive commit tuning
- [ ] Metrics and monitoring

---

## 11. References

- [RocksDB Wiki: Write Ahead Log](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log)
- [RocksDB Group Commit Implementation](https://github.com/facebook/rocksdb/blob/main/db/log_writer.cc)
- [Silkowski: Transaction Processing](https://www.amazon.com/Transaction-Processing-Concepts-Techniques-Management/dp/1558601902)