// Package backoff 是重连调度：指数退避 + 服务端 Retry-After + 全局每秒预算。
// 单次 Next 为 O(1)，状态只有每连接一个尝试计数，内存 O(连接数)。
package backoff

import (
	"hash/fnv"
	"strconv"
)

// BaseMs 是第一次退避时长，MaxMs 是退避上限，BudgetPerSec 是全局每秒放行次数。
const (
	BaseMs       = 500
	MaxMs        = 30000
	BudgetPerSec = 100
)

// capAttempt 是退避在最差抖动（-25%）下仍必然触顶 MaxMs 的最小尝试次数：
// BaseMs*2^(8-1)*3/4 = 48000 >= MaxMs，故第 8 次起结果恒为 MaxMs（防位移溢出）。
const capAttempt = 8

// Scheduler 是重连调度器。
type Scheduler struct {
	attempts map[string]int
	tokens   float64
	lastMs   int64
	started  bool
	work     int
}

// NewScheduler 建调度器，令牌桶初始为满。
func NewScheduler() *Scheduler {
	return &Scheduler{attempts: map[string]int{}, tokens: BudgetPerSec}
}

// Next 计算某个连接下一次重连的等待时间，并判断这次尝试能不能放行。
// 等待时间 = max(Retry-After, 封顶MaxMs(BaseMs*2^(n-1) ± 25%确定性抖动))。
// 全局令牌桶每秒补充 BudgetPerSec 个令牌；预算不足时返回 (0, false)，
// 且不消耗该连接的重试次数。
func (s *Scheduler) Next(connID string, nowMs int64, retryAfterMs int64) (int64, bool) {
	s.work++
	if !s.allow(nowMs) {
		return 0, false
	}
	attempt := s.attempts[connID] + 1
	s.attempts[connID] = attempt
	delay := backoffDelay(connID, attempt)
	if retryAfterMs > delay {
		delay = retryAfterMs
	}
	return delay, true
}

// Success 标记重连成功，之后从第一次退避重新开始。
func (s *Scheduler) Success(connID string) {
	delete(s.attempts, connID)
}

// Attempts 是当前连接的连续失败次数。
func (s *Scheduler) Attempts(connID string) int { return s.attempts[connID] }

// Work 是累计计算步数（规模观测）：每次 Next 固定一步，与尝试次数无关。
func (s *Scheduler) Work() int { return s.work }

// allow 是按墙钟时间补充令牌的令牌桶，单次 O(1)。
func (s *Scheduler) allow(nowMs int64) bool {
	if !s.started {
		s.started = true
		s.lastMs = nowMs
	} else if nowMs > s.lastMs {
		s.tokens += float64(nowMs-s.lastMs) * BudgetPerSec / 1000.0
		if s.tokens > BudgetPerSec {
			s.tokens = BudgetPerSec
		}
		s.lastMs = nowMs
	}
	if s.tokens < 1 {
		return false
	}
	s.tokens--
	return true
}

// backoffDelay 计算第 attempt 次尝试的退避：BaseMs*2^(attempt-1)，
// 叠加由 (connID, attempt) 派生的 ±25% 确定性抖动（同一连接同一尝试可复现），
// 抖动之后封顶 MaxMs。
func backoffDelay(connID string, attempt int) int64 {
	if attempt >= capAttempt {
		return MaxMs
	}
	base := int64(BaseMs) << uint(attempt-1)
	quarter := base / 4
	offset := int64(jitterHash(connID, attempt)%uint64(2*quarter+1)) - quarter
	delay := base + offset
	if delay > MaxMs {
		return MaxMs
	}
	return delay
}

// jitterHash 由连接 ID 与尝试次数派生确定性哈希（FNV-1a）。
func jitterHash(connID string, attempt int) uint64 {
	hasher := fnv.New64a()
	hasher.Write([]byte(connID))
	hasher.Write([]byte(strconv.Itoa(attempt)))
	return hasher.Sum64()
}
