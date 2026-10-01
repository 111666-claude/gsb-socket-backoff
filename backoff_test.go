package backoff

import "testing"

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
