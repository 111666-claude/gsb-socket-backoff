package backoff

import (
	"strconv"
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
		t.Fatalf("Retry-After 较大时应取服务端等待/口径值：%d %v", delay, allowed)
	}
	delay, _ = scheduler.Next("c1", 1000, 1)
	if delay < 750 || delay > 1250 {
		t.Fatalf("Retry-After 较小时应取退避值（第 2 次 1000±25%%）：%d", delay)
	}
}

func TestBudgetPerSecondGlobal(t *testing.T) {
	scheduler := NewScheduler()
	allowed := 0
	for index := 0; index < 200; index++ {
		if _, ok := scheduler.Next(connName(index), 0, 0); ok {
			allowed++
		}
	}
	if allowed != BudgetPerSec {
		t.Fatalf("同一秒最多放行 %d 次：%d", BudgetPerSec, allowed)
	}
	if scheduler.Attempts(connName(199)) != 0 {
		t.Fatal("被预算拒绝不该消耗重试次数")
	}
	if _, ok := scheduler.Next(connName(200), 0, 0); ok {
		t.Fatal("令牌耗尽后同一毫秒内不该再放行")
	}
	if _, ok := scheduler.Next(connName(200), 1000, 0); !ok {
		t.Fatal("一秒后令牌应补足并恢复放行")
	}
}

func TestCapAtMaxMs(t *testing.T) {
	scheduler := NewScheduler()
	for index := 0; index < 20; index++ {
		delay, allowed := scheduler.Next("c1", int64(index)*1000, 0)
		if !allowed {
			t.Fatalf("每秒一次不该触发预算：第 %d 次", index+1)
		}
		if delay > MaxMs {
			t.Fatalf("抖动之后仍要封顶 %d：第 %d 次 %d", MaxMs, index+1, delay)
		}
	}
	if delay, _ := scheduler.Next("c1", 20000, 0); delay != MaxMs {
		t.Fatalf("第 21 次必然封顶：%d", delay)
	}
}

func TestSuccessResets(t *testing.T) {
	scheduler := NewScheduler()
	scheduler.Next("c1", 0, 0)
	scheduler.Next("c1", 1000, 0)
	scheduler.Next("c1", 2000, 0)
	scheduler.Success("c1")
	if scheduler.Attempts("c1") != 0 {
		t.Fatal("Success 之后连续失败次数应清零")
	}
	delay, _ := scheduler.Next("c1", 100000, 0)
	if delay < 375 || delay > 625 {
		t.Fatalf("Success 之后应从第一次退避重算（500±25%%）：%d", delay)
	}
	if scheduler.Attempts("c1") != 1 {
		t.Fatalf("Success 之后应记为第 1 次失败：%d", scheduler.Attempts("c1"))
	}
}

func TestJitterDeterministicAndBounded(t *testing.T) {
	first := NewScheduler()
	second := NewScheduler()
	for attempt := 1; attempt <= 7; attempt++ {
		nowMs := int64(attempt) * 1000
		delayA, _ := first.Next("c1", nowMs, 0)
		delayB, _ := second.Next("c1", nowMs, 0)
		if delayA != delayB {
			t.Fatalf("同一连接同一尝试结果必须可复现：第 %d 次 %d != %d", attempt, delayA, delayB)
		}
		base := int64(BaseMs) << uint(attempt-1)
		if delayA < base*3/4 || delayA > base*5/4 {
			t.Fatalf("抖动必须落在 ±25%% 内：第 %d 次 %d（基数 %d）", attempt, delayA, base)
		}
	}
}

func TestWorkIsConstantPerCall(t *testing.T) {
	scheduler := NewScheduler()
	const calls = 100
	for index := 0; index < calls; index++ {
		scheduler.Next("c1", int64(index)*1000, 0)
	}
	if scheduler.Work() != calls {
		t.Fatalf("Work 不该随尝试次数放大：%d 次调用应只有 %d 步，实际 %d", calls, calls, scheduler.Work())
	}
}

func connName(index int) string {
	return "c-" + strconv.Itoa(index)
}
