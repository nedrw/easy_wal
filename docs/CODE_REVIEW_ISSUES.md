
### P3 - 缺少 sync 完成回调 ✅ 已完成

**位置**: `src/wal/coordinators.rs`

**问题**: `WriteCoordinator::do_sync` 执行 fsync 但没有回调钩子，外部无法感知同步完成。

**修复内容** (Rust 惯用风格):

1. 添加 `SyncReport` 结构体，包含同步结果信息：
   ```rust
   pub struct SyncReport {
       pub duration_ms: u64,      // 同步耗时（毫秒）
       pub success: bool,         // 是否成功
       pub error: Option<String>, // 错误信息（仅在 success 为 false 时有值）
   }
   ```

2. 修改 `sync()` 方法返回 `Result<SyncReport>` 而非 `Result<()>`：
   ```rust
   pub async fn sync(&self) -> Result<SyncReport> {
       self.do_sync().await
   }
   ```

3. 同步失败时仍返回 `Ok(SyncReport)`（报告失败状态），而非 `Err`：
   ```rust
   Err(e) => Ok(SyncReport {
       duration_ms,
       success: false,
       error: Some(e.to_string()),
   })
   ```

**使用示例**:
```rust
let coordinator = WriteCoordinator::new(writer, SyncMode::FsyncOnWrite);

// 写入数据（可能触发自动同步）
coordinator.write(b"data").await?;

// 手动同步并获取报告
let report = coordinator.sync().await?;

println!("Sync completed in {}ms, success: {}", report.duration_ms, report.success);
if !report.success {
    eprintln!("Sync error: {}", report.error.unwrap());
}
```

**设计说明**:
- **Rust 风格**：通过 `Result<SyncReport>` 直接返回结果，而非回调注入
- **失败不抛错**：同步失败仍返回 `Ok`，通过 `report.success` 判断，调用方不会因同步失败丢失数据
- **与 Kafka 对比**：Kafka 用 `Callback(success, error)` 双参数回调；Rust 用 `Result<Report>` 返回值，语义更清晰
