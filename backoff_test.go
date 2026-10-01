package backoff

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand"
	"testing"
)

func TestFirstAttemptIsAllowed(t *testing.T) {
	delay, allowed := NewScheduler().Next("c1", 0, 0)
	if !allowed || delay <= 0 {
		t.Fatalf("第一次尝试应该被放行并给出正等待时间：%d %v", delay, allowed)
	}
}

func TestAttemptsCountUp(t *testing.T) {
	scheduler := NewScheduler()
	scheduler.Next("c1", 0, 0)
	scheduler.Next("c1", 1000, 0)
	if scheduler.Attempts("c1") != 2 {
		t.Fatalf("失败次数应该累加：%d", scheduler.Attempts("c1"))
	}
}

func TestConnectionsAreIndependent(t *testing.T) {
	scheduler := NewScheduler()
	scheduler.Next("c1", 0, 0)
	if scheduler.Attempts("c2") != 0 {
		t.Fatal("不同连接之间不该互相影响")
	}
}

func TestConstants(t *testing.T) {
	if BaseMs != 500 || MaxMs != 30000 || BudgetPerSec != 100 {
		t.Fatal("退避常量被改了")
	}
}

func TestRetryAfterWins(t *testing.T) {
	scheduler := NewScheduler()
	delay, allowed := scheduler.Next("c1", 0, 60000)
	if !allowed || delay != 60000 {
		t.Fatalf("服务端 Retry-After 应该优先：delay=%d allowed=%v", delay, allowed)
	}
	delay, _ = scheduler.Next("c1", 1000, 1)
	if delay <= 1 {
		t.Fatalf("Retry-After 比退避小时应该取退避：delay=%d", delay)
	}
}

func TestGlobalBudget(t *testing.T) {
	scheduler := NewScheduler()
	allowed := 0
	for index := 0; index < 200; index++ {
		if _, ok := scheduler.Next(string(rune('a'+index/26))+string(rune('a'+index%26)), 0, 0); ok {
			allowed++
		}
	}
	if allowed != BudgetPerSec {
		t.Fatalf("同一秒最多放行 %d 次，实际 %d", BudgetPerSec, allowed)
	}
	if scheduler.Attempts("zz") != 0 {
		t.Fatalf("被预算拒绝不该消耗重试次数：%d", scheduler.Attempts("zz"))
	}
	refilled := 0
	for index := 0; index < 200; index++ {
		if _, ok := scheduler.Next(string(rune('A'+index/26))+string(rune('A'+index%26)), 1000, 0); ok {
			refilled++
		}
	}
	if refilled != BudgetPerSec {
		t.Fatalf("一秒后应该再补充 %d 个令牌，实际放行 %d", BudgetPerSec, refilled)
	}
}

func TestCapAtMaxMs(t *testing.T) {
	scheduler := NewScheduler()
	delay := int64(0)
	for index := 0; index < 20; index++ {
		delay, _ = scheduler.Next("c1", int64(index)*1000, 0)
	}
	if delay != MaxMs {
		t.Fatalf("退避应该封顶在 %d，实际 %d", MaxMs, delay)
	}
}

func TestSuccessResets(t *testing.T) {
	scheduler := NewScheduler()
	scheduler.Next("c1", 0, 0)
	scheduler.Next("c1", 1000, 0)
	scheduler.Next("c1", 2000, 0)
	scheduler.Success("c1")
	if scheduler.Attempts("c1") != 0 {
		t.Fatalf("成功后失败次数应该清零：%d", scheduler.Attempts("c1"))
	}
	delay, _ := scheduler.Next("c1", 100000, 0)
	if delay < 375 || delay > 625 {
		t.Fatalf("成功后应该从第一次退避重算（500±25%%），实际 %d", delay)
	}
}

func TestJitterIsDeterministicAndBounded(t *testing.T) {
	first := NewScheduler()
	second := NewScheduler()
	for attempt := 1; attempt <= 8; attempt++ {
		nowMs := int64(attempt-1) * 1000
		delayA, _ := first.Next("conn-x", nowMs, 0)
		delayB, _ := second.Next("conn-x", nowMs, 0)
		if delayA != delayB {
			t.Fatalf("同一连接同一尝试的退避必须可复现：%d != %d", delayA, delayB)
		}
		base := int64(BaseMs) << (attempt - 1)
		if base > MaxMs {
			if delayA != MaxMs {
				t.Fatalf("第 %d 次应该封顶 %d，实际 %d", attempt, MaxMs, delayA)
			}
			continue
		}
		if delayA < base*3/4 || delayA > base*5/4 {
			t.Fatalf("第 %d 次抖动超出 ±25%%：base=%d delay=%d", attempt, base, delayA)
		}
	}
}

func TestWorkIsConstantPerNext(t *testing.T) {
	scheduler := NewScheduler()
	const calls = 200000
	for index := 0; index < calls; index++ {
		connID := string(rune(index%50000 + 1))
		scheduler.Next(connID, int64(index), 0)
	}
	if scheduler.Work() != calls {
		t.Fatalf("单次 Next 应该 O(1)：%d 次调用走了 %d 步", calls, scheduler.Work())
	}
}

// referenceScheduler 是「指数退避 + 抖动 + 预算 + 封顶」的参考实现，
// 刻意用直白的重放式写法，与 Scheduler 的 O(1) 实现对照。
type referenceScheduler struct {
	attempts map[string]int
	tokens   float64
	lastMs   int64
	started  bool
}

func newReferenceScheduler() *referenceScheduler {
	return &referenceScheduler{attempts: map[string]int{}, tokens: BudgetPerSec}
}

func referenceJitter(connID string, attempt int) float64 {
	hash := fnv.New64a()
	hash.Write([]byte(connID))
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(attempt))
	hash.Write(buf[:])
	unit := float64(hash.Sum64()>>11) / float64(uint64(1)<<53)
	return (unit - 0.5) * 0.5
}

func (r *referenceScheduler) next(connID string, nowMs int64, retryAfterMs int64) (int64, bool) {
	if !r.started {
		r.started = true
		r.lastMs = nowMs
	} else if elapsed := nowMs - r.lastMs; elapsed > 0 {
		r.tokens += float64(elapsed) * BudgetPerSec / 1000
		if r.tokens > BudgetPerSec {
			r.tokens = BudgetPerSec
		}
		r.lastMs = nowMs
	}
	if r.tokens < 1 {
		return 0, false
	}
	r.tokens--
	attempt := r.attempts[connID] + 1
	r.attempts[connID] = attempt
	base := float64(BaseMs)
	for index := 1; index < attempt; index++ {
		base *= 2
	}
	delay := base * (1 + referenceJitter(connID, attempt))
	capped := int64(delay)
	if delay >= MaxMs {
		capped = MaxMs
	}
	if retryAfterMs > capped {
		capped = retryAfterMs
	}
	return capped, true
}

func (r *referenceScheduler) success(connID string) {
	delete(r.attempts, connID)
}

func TestMatchesReferenceImplementation(t *testing.T) {
	rng := rand.New(rand.NewSource(20261001))
	mismatches := 0
	for round := 0; round < 200; round++ {
		actual := NewScheduler()
		reference := newReferenceScheduler()
		connCount := 1 + rng.Intn(64)
		conns := make([]string, connCount)
		for index := range conns {
			conns[index] = "conn-" + string(rune(index+1)) + "-" + string(rune(round+1))
		}
		nowMs := rng.Int63n(1000)
		events := 50 + rng.Intn(200)
		for event := 0; event < events; event++ {
			nowMs += rng.Int63n(3000)
			connID := conns[rng.Intn(connCount)]
			if rng.Intn(5) == 0 {
				actual.Success(connID)
				reference.success(connID)
				continue
			}
			retryAfterMs := int64(0)
			if rng.Intn(3) == 0 {
				retryAfterMs = rng.Int63n(90000)
			}
			delayA, allowedA := actual.Next(connID, nowMs, retryAfterMs)
			delayB, allowedB := reference.next(connID, nowMs, retryAfterMs)
			if delayA != delayB || allowedA != allowedB {
				mismatches++
				t.Errorf("round=%d event=%d conn=%q now=%d retryAfter=%d：实现 (%d,%v) != 参考 (%d,%v)",
					round, event, connID, nowMs, retryAfterMs, delayA, allowedA, delayB, allowedB)
			}
		}
	}
	if mismatches != 0 {
		t.Fatalf("与参考实现的差异必须为 0，实际 %d", mismatches)
	}
}
