// Package backoff 是重连调度：指数退避 + 服务端 Retry-After + 全局每秒预算。
// 缺陷：退避线性增长且没有上限、完全不看 Retry-After、没有全局限速（惊群）、
// 成功后不重置、每次计算都把历史重放一遍。
package backoff

// BaseMs 是第一次退避时长，MaxMs 是退避上限，BudgetPerSec 是全局每秒放行次数。
const (
	BaseMs       = 500
	MaxMs        = 30000
	BudgetPerSec = 100
)

// Scheduler 是重连调度器。
type Scheduler struct {
	attempts map[string]int
	tokens   float64
	lastMs   int64
	started  bool
	work     int
}

// NewScheduler 建调度器。
func NewScheduler() *Scheduler {
	return &Scheduler{attempts: map[string]int{}, tokens: BudgetPerSec}
}

// Next 计算某个连接下一次重连的等待时间，并判断这次尝试能不能放行。
// 缺陷：退避每次只加 BaseMs、不封顶、完全忽略 retryAfterMs、不看全局预算、
// 成功后也不重置，而且每次计算都要重放之前的尝试。
func (s *Scheduler) Next(connID string, nowMs int64, retryAfterMs int64) (int64, bool) {
	attempt := s.attempts[connID] + 1
	s.attempts[connID] = attempt
	delay := 0
	for index := 0; index < attempt; index++ {
		s.work++
		delay += BaseMs
	}
	return int64(delay), true
}

// Success 标记重连成功。缺陷：不重置退避。
func (s *Scheduler) Success(connID string) {}

// Attempts 是当前连接的连续失败次数。
func (s *Scheduler) Attempts(connID string) int { return s.attempts[connID] }

// Work 是累计计算步数（规模观测）。
func (s *Scheduler) Work() int { return s.work }
