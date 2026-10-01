package backoff

import "testing"

func TestFirstDelayIsBase(t *testing.T) {
	if got := NewScheduler().Next("c1"); got != BaseMs {
		t.Fatalf("第一次退避应该是 BaseMs：%d", got)
	}
}

func TestAttemptsCountUp(t *testing.T) {
	scheduler := NewScheduler()
	scheduler.Next("c1")
	scheduler.Next("c1")
	if scheduler.Attempts("c1") != 2 {
		t.Fatalf("失败次数应该累加：%d", scheduler.Attempts("c1"))
	}
}

func TestConnectionsAreIndependent(t *testing.T) {
	scheduler := NewScheduler()
	scheduler.Next("c1")
	if scheduler.Attempts("c2") != 0 {
		t.Fatal("不同连接之间不该互相影响")
	}
}

func TestConstants(t *testing.T) {
	if BaseMs != 500 || MaxMs != 30000 {
		t.Fatal("退避常量被改了")
	}
}
