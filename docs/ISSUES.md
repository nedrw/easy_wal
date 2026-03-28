# easy_wal 问题跟踪

## 待修复

| # | 问题 | 优先级 | 说明 |
|---|------|--------|------|
| 6 | `write_batch` offset 计算错误 | 高 | write 返回 offset=28，write_batch 返回 offset=49 |

## 已接受/忽略

- #3: seek 不返回 Result 是设计选择
- #4: 误报，has_incomplete 字段有使用
- #7: prefix 为空时仍能正确解析
- #8: 单进程使用无需文件锁
- #11: 临时文件残留影响不大