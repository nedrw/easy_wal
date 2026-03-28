# easy_wal 问题跟踪

## 待修复

暂无待修复问题。

## 已修复

| # | 问题 | 修复说明 |
|---|------|----------|
| 6 | `write_batch` offset 计算错误 | 测试设计问题。使用两个独立的 WAL 实例比较，验证了 `write` 和 `write_batch` 都返回正确的数据起始位置（28）。`write_batch` 中的 `offset + RECORD_HEADER_SIZE` 计算是正确的，确保返回的 offset 指向数据起始位置（记录头之后），与 `write` 方法保持一致。 |

## 已接受/忽略

- #3: seek 不返回 Result 是设计选择
- #4: 误报，has_incomplete 字段有使用
- #7: prefix 为空时仍能正确解析
- #8: 单进程使用无需文件锁
- #11: 临时文件残留影响不大