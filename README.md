# socket-backoff

重连退避：指数退避、上限封顶、失败重置，每次计算常数次运算，只用 Go 标准库。

```
go test ./...
go run ./cmd/socket-backoff --sample growth
go run ./cmd/socket-backoff --sample cap
go run ./cmd/socket-backoff --sample reset
```

## 口径（README 为准）

- **指数退避**：第 n 次失败的退避是 `min(MaxMs, BaseMs * 2^(n-1))`。
- **上限封顶**：继续失败也不会超过 `MaxMs`。
- **成功重置**：`Success` 之后从第一次退避重新开始。
- **计算代价**：单次计算是常数次运算，`Work` 不随尝试次数放大。
- **规模**：单服 50 万连接、每秒 20 万次退避计算，单次 O(1)，内存 O(连接数)。

## 输出契约（不改格式）

```
delays=500,1000,2000,4000
delay=30000
delay=500
work<=1000
```

## 常量

```
BaseMs = 500
MaxMs  = 30000
```

## 目录

```
backoff.go              退避计算
cmd/socket-backoff      命令行入口
backoff_test.go         go test 用例
```
