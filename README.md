# socket-backoff

重连调度：指数退避 + 服务端 Retry-After + 全局每秒预算，只用 Go 标准库。

```
go test ./...
go run ./cmd/socket-backoff --sample retry-after
go run ./cmd/socket-backoff --sample budget
go run ./cmd/socket-backoff --sample cap
```

## 口径（README 为准）

- **指数退避带确定性抖动**：第 n 次尝试的基础退避是 `BaseMs * 2^(n-1)`，
  叠加由 `(connID, n)` 派生的抖动（±25%），同一连接同一尝试的结果必须可复现。
- **Retry-After 优先**：服务端给了 `retryAfterMs` 时，实际等待时间取两者的较大值。
- **全局预算**：每秒最多放行 `BudgetPerSec` 次尝试（按时间补充令牌）；
  超出预算时返回 `false`，并且不消耗这位连接的重试次数。
- **上限封顶**：抖动之后仍要封顶在 `MaxMs`。
- **成功重置**：`Success` 之后从第一次退避重新开始。
- **规模**：50 万连接、每秒 20 万次计算，单次 O(1)，内存 O(连接数)。

## 输出契约（不改格式）

```
delay=60000
allowed=100
delay=30000
in_range=true
```

## 常量

```
BaseMs       = 500
MaxMs        = 30000
BudgetPerSec = 100
```

## 目录

```
backoff.go              退避与预算
cmd/socket-backoff      命令行入口
backoff_test.go         go test 用例
```
