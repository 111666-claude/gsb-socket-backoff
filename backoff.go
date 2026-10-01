// Package backoff 是重连调度：指数退避 + 确定性抖动 + 服务端 Retry-After +
// 全局每秒预算。单次 Next 是 O(1)，内存 O(连接数)。
package backoff

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

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
// 等待时间是指数退避加确定性抖动、封顶 MaxMs 之后，再与 retryAfterMs 取较大者。
// 全局令牌预算不足时返回 false，且不消耗该连接的重试次数。
func (s *Scheduler) Next(connID string, nowMs int64, retryAfterMs int64) (int64, bool) {
	s.work++
	s.refill(nowMs)
	if s.tokens < 1 {
		return 0, false
	}
	s.tokens--
	attempt := s.attempts[connID] + 1
	s.attempts[connID] = attempt
	delay := backoffDelay(connID, attempt)
	if retryAfterMs > delay {
		delay = retryAfterMs
	}
	return delay, true
}

// Success 标记重连成功，该连接的退避从第一次重新算起。
func (s *Scheduler) Success(connID string) {
	delete(s.attempts, connID)
}

// Attempts 是当前连接的连续失败次数。
func (s *Scheduler) Attempts(connID string) int { return s.attempts[connID] }

// Work 是累计计算步数（规模观测），每次 Next 只走一步，O(1)。
func (s *Scheduler) Work() int { return s.work }

// backoffDelay 是第 attempt 次尝试的退避：BaseMs * 2^(attempt-1) 加抖动，封顶 MaxMs。
func backoffDelay(connID string, attempt int) int64 {
	base := float64(BaseMs) * math.Exp2(float64(attempt-1))
	delay := base * (1 + jitter(connID, attempt))
	if delay >= MaxMs {
		return MaxMs
	}
	return int64(delay)
}

// jitter 返回由 (connID, attempt) 派生的确定性抖动，范围 [-0.25, +0.25)，
// 同一连接同一尝试的结果可复现。
func jitter(connID string, attempt int) float64 {
	hash := fnv.New64a()
	hash.Write([]byte(connID))
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(attempt))
	hash.Write(buf[:])
	unit := float64(hash.Sum64()>>11) / float64(uint64(1)<<53)
	return (unit - 0.5) * 0.5
}

// refill 按经过的时间补充全局令牌，封顶 BudgetPerSec。
func (s *Scheduler) refill(nowMs int64) {
	if !s.started {
		s.started = true
		s.lastMs = nowMs
		return
	}
	elapsed := nowMs - s.lastMs
	if elapsed <= 0 {
		return
	}
	s.tokens += float64(elapsed) * BudgetPerSec / 1000
	if s.tokens > BudgetPerSec {
		s.tokens = BudgetPerSec
	}
	s.lastMs = nowMs
}
