# WAL 格式设计文档

## 1. 概述

本文档描述 WAL (Write-Ahead Logging) 的磁盘格式设计，包括当前格式的问题分析、以及引入 Magic Number 的改进方案。

## 2. 当前格式

### 2.1 现状

```/dev/null/current_format.rs#L1-3
// 当前格式：[length(8)][data...]
// 8 字节长度前缀（大端序），后面跟着实际数据

let length = u64::from_be_bytes([
    length_bytes[0], length_bytes[1], length_bytes[2], length_bytes[3],
    length_bytes[4], length_bytes[5], length_bytes[6], length_bytes[7],
]);
let data = storage.read(offset + 8, length).await?;
```

### 2.2 问题

| 问题 | 影响 |
|------|------|
| **无边界标记** | 无法区分"真正的长度前缀"和"恰好像长度的垃圾数据" |
| **无校验** | 数据损坏无法检测 |
| **无版本** | 无法演进格式 |
| **O(n²) 扫描** | Recovery 时 offset += 1 逐字节前进 |

```/dev/null/recovery_problem.rs#L1-15
// 当前 Recovery 扫描逻辑
while offset + 8 <= file_size {
    match self.verify_record(&storage, offset).await {
        Ok(true) => {
            // 读取长度获取下一条记录位置
            offset += 8 + length;
        }
        Ok(false) => {
            // 记录损坏，无法确认边界
            // 只能逐字节前进尝试寻找下一个可能的记录
            corrupted_skipped += 1;
            offset += 1;  // ⚠️ O(n²) 根源
        }
        Err(_) => break,
    }
}
```

---

## 3. Magic Number 方案

### 3.1 格式设计

```/dev/null/proposed_format.rs#L1-14
// 改进后的格式：
// [magic(8)][version(1)][length(8)][data...][magic(8)]
// ↑----------头部--------↑  ↑---数据---↑  ↑--尾部--↑

const RECORD_MAGIC: u64 = 0xDEADBEEF_CAFEBABE;
const WAL_VERSION: u8 = 1;

const RECORD_HEADER_SIZE: u64 = 17;  // magic(8) + version(1) + length(8)
const RECORD_FOOTER_SIZE: u64 = 8;   // 尾部 magic(8)

// 完整记录大小 = 17 + length + 8
```

### 3.2 字段说明

| 字段 | 大小 | 说明 |
|------|------|------|
| `magic` | 8 bytes | 记录边界标记，固定值 0xDEADBEEF_CAFEBABE |
| `version` | 1 byte | 格式版本号，支持未来演进 |
| `length` | 8 bytes | 数据长度（大端序） |
| `data` | variable | 实际数据 |
| `footer_magic` | 8 bytes | 尾部 magic，与头部相同 |

### 3.3 结构体定义

```rust
/// WAL 记录头部
#[repr(C)]
struct RecordHeader {
    /// 魔数，固定 0xDEADBEEF_CAFEBABE
    magic: u64,
    /// WAL 格式版本
    version: u8,
    /// 数据长度
    length: u32,
    /// CRC32 校验和（可选，Phase 6 添加）
    #[allow(dead_code)]
    crc32: u32,
}

/// WAL 记录尾部
#[repr(C)]
struct RecordFooter {
    /// 魔数，与头部相同
    magic: u64,
}
```

---

## 4. 优势分析

### 4.1 O(n) 扫描

```/dev/null/scan_logic.rs#L1-25
// 使用 Magic Number 后的扫描逻辑
while offset + 17 <= file_size {
    // 读取头部
    let header = storage.read(offset, 17).await?;
    
    // 验证魔数
    let magic = u64::from_be_bytes(header[0..8].try_into().unwrap());
    if magic != RECORD_MAGIC {
        offset += 1;  // 未找到记录边界
        continue;
    }
    
    // 验证版本
    let version = header[8];
    if version != WAL_VERSION {
        return Err(Error::UnsupportedVersion(version));
    }
    
    // 读取长度
    let length = u32::from_be_bytes(header[9..13].try_into().unwrap());
    
    // 验证数据 + 尾部 magic
    let data = storage.read(offset + 17, length as u64).await?;
    let footer = storage.read(offset + 17 + length as u64, 8).await?;
    let footer_magic = u64::from_be_bytes(footer.try_into().unwrap());
    
    if footer_magic != RECORD_MAGIC {
        offset += 1;  // 数据损坏或截断
        continue;
    }
    
    // 完整记录，跳跃到下一条
    offset += 17 + length as u64 + 8;
    records_found += 1;
}
```

**复杂度对比**：

| 方案 | 时间复杂度 | 最坏情况 |
|------|-----------|----------|
| 当前（逐字节） | O(n²) | n 次损坏，每次扫描 n 字节 |
| Magic Number | O(n) | 每次跳转完整记录大小 |

### 4.2 快速定位

```rust
// 可以直接跳过整个记录
offset += RECORD_HEADER_SIZE + record.length as u64 + RECORD_FOOTER_SIZE;
```

### 4.3 版本演进支持

```rust
match header.version {
    1 => decode_v1(&header, &data)?,
    2 => decode_v2(&header, &data)?,
    // Future versions...
}
```

---

## 5. 与主流方案对比

| 库 | 边界标记 | CRC | Magic | 特点 |
|----|----------|-----|-------|------|
| etcd | 固定头 | ✅ | ❌ | term + type + crc |
| tikv | 固定头 | ✅ | ❌ | 最简设计 |
| RocksDB | 块+类型 | ✅ | ❌ | 记录分片 |
| PostgreSQL | 块对齐 | ✅ | ❌ | 事务ID |
| **本文档** | **Magic** | **✅** | **✅** | **双 Magic** |

---

## 6. 实现计划

### 6.1 Phase 6（可靠性增强）

1. **格式升级**
   - 添加 Magic Number 头部和尾部
   - 添加 version 字段
   - 添加 CRC32 校验（可选）

2. **兼容性**
   - 支持读取旧格式（检测 magic 缺失）
   - 新写入使用新格式

3. **迁移工具**
   - 提供格式转换脚本
   - 支持批量迁移历史数据

### 6.2 代码示例

```rust
// 构建记录
fn build_record(data: &[u8]) -> Vec<u8> {
    let mut record = Vec::with_capacity(
        17 + data.len() + 8
    );
    
    // 头部 magic
    record.extend_from_slice(&RECORD_MAGIC.to_be_bytes());
    // version
    record.push(WAL_VERSION);
    // length
    record.extend_from_slice(&(data.len() as u32).to_be_bytes());
    // crc32 (Phase 6)
    record.extend_from_slice(&0u32.to_be_bytes());  // 占位
    
    // 数据
    record.extend_from_slice(data);
    
    // 尾部 magic
    record.extend_from_slice(&RECORD_MAGIC.to_be_bytes());
    
    record
}

// 验证记录
fn verify_record(raw: &[u8]) -> Option<&[u8]> {
    if raw.len() < 17 + 8 {
        return None;
    }
    
    // 检查头部 magic
    let magic = u64::from_be_bytes(raw[0..8].try_into().unwrap());
    if magic != RECORD_MAGIC {
        return None;
    }
    
    // 检查 version
    let version = raw[8];
    if version != WAL_VERSION {
        return None;
    }
    
    // 读取长度
    let length = u32::from_be_bytes(raw[9..13].try_into().unwrap()) as usize;
    
    // 检查尾部 magic
    let footer_off = 17 + length;
    let footer = u64::from_be_bytes(raw[footer_off..footer_off+8].try_into().unwrap());
    if footer != RECORD_MAGIC {
        return None;
    }
    
    // 返回数据部分
    Some(&raw[17..17+length])
}
```

---

## 7. 注意事项

### 7.1 破坏性变更

> ⚠️ **警告**：此格式变更会破坏向后兼容性。

- 旧格式 WAL 文件需要迁移
- 迁移期间服务需要停机或只读
- 建议保留旧格式读取支持

### 7.2 Magic Number 选择

```rust
// 好：明显的标记，不易与数据混淆
const RECORD_MAGIC: u64 = 0xDEADBEEF_CAFEBABE;

// 好：64位随机数，碰撞概率极低
const RECORD_MAGIC: u64 = 0xA1B2C3D4_E5F6G7H8;

// 差：太短或太简单
const BAD_MAGIC: u32 = 0xDEADBEEF;  // 32位碰撞概率高
```

### 7.3 性能考虑

- 读取时需要验证两个 magic（头部+尾部）
- 可考虑只验证头部，尾部在写入时校验（Write-Once）

---

## 8. 参考资料

- [etcd WAL Design](https://github.com/etcd-io/etcd/blob/main/wal/wal.go)
- [TiKV WAL Implementation](https://github.com/tikv/tikv/blob/master/components/raftstore/src/wal.rs)
- [RocksDB Log Format](https://github.com/facebook/rocksdb/blob/main/db/log_format.h)
- [PostgreSQL WAL](https://github.com/postgres/postgres/blob/master/src/include/access/xlogdefs.h)

---

## 9. 修订历史

| 版本 | 日期 | 说明 |
|------|------|------|
| 1.0 | 2024-XX-XX | 初始文档 |