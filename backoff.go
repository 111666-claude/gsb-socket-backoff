// Package backoff 是重连退避：指数退避、上限封顶、失败重置，每次计算常数次运算。
// 缺陷：退避线性增长、没有上限、成功后不重置、每次计算都把之前的尝试重放一遍。
package backoff

// BaseMs 是第一次退避时长，MaxMs 是退避上限。
const (
	BaseMs = 500
	MaxMs  = 30000
)

// Scheduler 是每个连接的重连退避台账。
type Scheduler struct {
	attempts map[string]int
	work     int
}

// NewScheduler 建台账。
func NewScheduler() *Scheduler {
	return &Scheduler{attempts: map[string]int{}}
}

// Next 返回下一次重连的退避时长（毫秒）。
// 缺陷：线性增长（每次加 BaseMs）、没有上限、每次都要重放整段历史。
func (s *Scheduler) Next(connID string) int {
	attempt := s.attempts[connID] + 1
	s.attempts[connID] = attempt
	delay := 0
	for index := 0; index < attempt; index++ {
		s.work++
		delay += BaseMs
	}
	return delay
}

// Success 标记一次重连成功。缺陷：不重置退避。
func (s *Scheduler) Success(connID string) {}

// Attempts 是当前连接的连续失败次数。
func (s *Scheduler) Attempts(connID string) int { return s.attempts[connID] }

// Work 是累计的计算步数（规模观测）。
func (s *Scheduler) Work() int { return s.work }
