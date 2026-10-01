package backoff

import (
	"hash/fnv"
	"math/rand"
	"strconv"
	"testing"
)

// referenceScheduler 是按 README 口径独立书写的参考实现：
// 指数退避（乘法循环展开）+ FNV-1a 确定性抖动（±25%）
// + 全局令牌预算（按毫秒补充）+ MaxMs 封顶 + 成功重置。
type referenceScheduler struct {
	attempts map[string]int
	tokens   float64
	lastMs   int64
	started  bool
}

func newReference() *referenceScheduler {
	return &referenceScheduler{attempts: map[string]int{}, tokens: BudgetPerSec}
}

func (r *referenceScheduler) next(connID string, nowMs int64, retryAfterMs int64) (int64, bool) {
	if !r.started {
		r.started = true
		r.lastMs = nowMs
	} else if nowMs > r.lastMs {
		r.tokens += float64(nowMs-r.lastMs) * BudgetPerSec / 1000
		if r.tokens > BudgetPerSec {
			r.tokens = BudgetPerSec
		}
		r.lastMs = nowMs
	}
	if r.tokens < 1 {
		return 0, false
	}
	r.tokens--
	n := r.attempts[connID] + 1
	r.attempts[connID] = n

	base := int64(BaseMs)
	for index := 1; index < n; index++ {
		base *= 2
	}
	var delay int64
	if base-base/4 >= MaxMs {
		delay = MaxMs
	} else {
		hasher := fnv.New64a()
		hasher.Write([]byte(connID))
		hasher.Write([]byte(strconv.Itoa(n)))
		span := base / 2
		delay = base - base/4 + int64(hasher.Sum64()%uint64(span+1))
		if delay > MaxMs {
			delay = MaxMs
		}
	}
	if retryAfterMs > delay {
		delay = retryAfterMs
	}
	return delay, true
}

func (r *referenceScheduler) success(connID string) {
	delete(r.attempts, connID)
}

// TestReferenceParity 随机 200 组「失败与成功交替 + 服务端等待时间 + 并发连接」，
// 与参考实现逐步比对等待时间、放行结果和最终失败次数，差异必须为 0。
func TestReferenceParity(t *testing.T) {
	rng := rand.New(rand.NewSource(20261001))
	for scenario := 0; scenario < 200; scenario++ {
		actual := NewScheduler()
		reference := newReference()
		conns := 1 + rng.Intn(20)
		nowMs := int64(0)
		steps := 50 + rng.Intn(300)
		nextCalls := 0
		for step := 0; step < steps; step++ {
			nowMs += int64(rng.Intn(3000))
			connID := "conn-" + strconv.Itoa(rng.Intn(conns))
			if rng.Intn(10) == 0 {
				actual.Success(connID)
				reference.success(connID)
				continue
			}
			retryAfterMs := int64(0)
			if rng.Intn(2) == 0 {
				retryAfterMs = int64(rng.Intn(70000))
			}
			delayA, allowedA := actual.Next(connID, nowMs, retryAfterMs)
			delayB, allowedB := reference.next(connID, nowMs, retryAfterMs)
			nextCalls++
			if delayA != delayB || allowedA != allowedB {
				t.Fatalf("场景 %d 第 %d 步 conn=%s now=%d retryAfter=%d："+
					"实际 (%d,%v) 参考 (%d,%v)",
					scenario, step, connID, nowMs, retryAfterMs,
					delayA, allowedA, delayB, allowedB)
			}
		}
		if actual.Work() != nextCalls {
			t.Fatalf("场景 %d Work 应为 Next 调用数 %d，实际 %d",
				scenario, nextCalls, actual.Work())
		}
		for index := 0; index < conns; index++ {
			connID := "conn-" + strconv.Itoa(index)
			if actual.Attempts(connID) != reference.attempts[connID] {
				t.Fatalf("场景 %d 连接 %s 失败次数不一致：实际 %d 参考 %d",
					scenario, connID, actual.Attempts(connID), reference.attempts[connID])
			}
		}
	}
}
