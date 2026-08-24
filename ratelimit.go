package pipeline

import (
	"context"
	"sync"
	"time"
)

// RateLimiter 是令牌桶限流器（D-29）。
//
// 支持两种调用语义：
//   - Allow：取一个令牌，有则立即返回 true；无则返回 false（调用方自行处理，如跳过/进死信）；
//   - Wait：阻塞直到取到令牌或 ctx 取消（保持顺序，适用于不可丢消息的背压式限流）。
//
// 并发安全：多个 worker 可同时调用。
type RateLimiter struct {
	rate   float64   // 每秒补充令牌数
	burst  float64   // 桶容量（突发上限）

	mu sync.Mutex
	tokens float64   // 当前令牌数
	last   time.Time // 上次补充时间
}

// NewRateLimiter 创建令牌桶限流器。
// rate 为每秒补充令牌数（>0），burst 为桶容量（突发上限，>=1）。
// 首桶默认满（允许立即突发 burst 个）。
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	if rate <= 0 {
		rate = 1
	}
	if burst < 1 {
		burst = 1
	}
	now := time.Now()
	return &RateLimiter{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst), // 初始满桶
		last:   now,
	}
}

// Allow 尝试取一个令牌。成功返回 true；令牌不足返回 false（不阻塞）。
func (rl *RateLimiter) Allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	return rl.take()
}

// Wait 阻塞直到取到令牌或 ctx 取消。
// 返回 nil 表示已取到令牌；ctx 取消时返回 ctx.Err()。
func (rl *RateLimiter) Wait(ctx context.Context) error {
	for {
		rl.mu.Lock()
		if rl.take() {
			rl.mu.Unlock()
			return nil
		}
		// 需等待：还差 (1-tokens) 个令牌（take 失败时 tokens ∈ [0,1)），按 rate 补充速率计算等待时长。
		waitNs := time.Duration(float64(1-rl.tokens) / rl.rate * float64(time.Second))
		rl.mu.Unlock()
		if waitNs < 0 {
			waitNs = 0
		}
		timer := time.NewTimer(waitNs)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
			timer.Stop()
			// 时间到，继续尝试。
		}
	}
}

// take 在已持锁前提下补充令牌并尝试取一个。成功返回 true。
func (rl *RateLimiter) take() bool {
	now := time.Now()
	elapsed := now.Sub(rl.last).Seconds()
	rl.last = now
	if elapsed > 0 {
		rl.tokens += elapsed * rl.rate
		if rl.tokens > rl.burst {
			rl.tokens = rl.burst
		}
	}
	if rl.tokens >= 1 {
		rl.tokens--
		return true
	}
	return false
}