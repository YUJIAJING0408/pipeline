package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestRateLimiterAllowBurst 验证首桶满：允许立即突发 burst 个。
func TestRateLimiterAllowBurst(t *testing.T) {
	rl := NewRateLimiter(100, 3)
	// 首桶满（3 个令牌）：连续 3 次应全部通过。
	for i := 0; i < 3; i++ {
		if !rl.Allow() {
			t.Fatalf("第 %d 次应通过（首桶满）", i+1)
		}
	}
	// 第 4 次应失败（令牌耗尽，需等待补充）。
	if rl.Allow() {
		t.Fatal("第 4 次应被拒绝（令牌耗尽）")
	}
}

// TestRateLimiterRefill 验证令牌按速率补充。
func TestRateLimiterRefill(t *testing.T) {
	rl := NewRateLimiter(1000, 1) // 1000/s = 1ms 补充 1 个
	if !rl.Allow() {
		t.Fatal("首桶应满")
	}
	if rl.Allow() {
		t.Fatal("第二个应立即被拒")
	}
	// 等 ~2ms，应有 2 个令牌补充。
	time.Sleep(2 * time.Millisecond)
	if !rl.Allow() {
		t.Fatal("补充后应可再次通过")
	}
}

// TestRateLimiterWait 验证 Wait 阻塞直到令牌可用。
func TestRateLimiterWait(t *testing.T) {
	rl := NewRateLimiter(1000, 1) // 1ms 补 1 个
	ctx := context.Background()
	start := time.Now()
	if err := rl.Wait(ctx); err != nil {
		t.Fatalf("Wait 失败: %v", err)
	}
	if err := rl.Wait(ctx); err != nil {
		t.Fatalf("第二次 Wait 失败: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < time.Millisecond {
		t.Errorf("两次 Wait 应至少阻塞 1ms（补充时间），实际 %v", elapsed)
	}
}

// TestRateLimiterWaitCancel 验证 Wait 在 ctx 取消时返回错误。
func TestRateLimiterWaitCancel(t *testing.T) {
	rl := NewRateLimiter(1, 1) // 1/s，补充极慢
	if err := rl.Wait(context.Background()); err != nil {
		t.Fatalf("首桶 Wait 失败: %v", err)
	}
	// 第二个令牌需等 1s；先取消 ctx。
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 预取消
	if err := rl.Wait(ctx); err == nil {
		t.Fatal("ctx 已取消，Wait 应返回错误")
	}
}

// TestRateLimiterConcurrent 验证并发安全：多 goroutine 同时 Allow 不超发。
func TestRateLimiterConcurrent(t *testing.T) {
	rl := NewRateLimiter(1000, 50) // 首桶 50，之后极快补充
	var passed int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	const total = 500

	for i := 0; i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rl.Allow() {
				mu.Lock()
				passed++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// 并发下不应超发——首桶 50 + 并发期间补充，但绝不能超过 total。
	if passed < 50 {
		t.Errorf("并发通过数 = %d, 至少应放行 50（首桶）", passed)
	}
}

// TestRateLimiterStage 验证 Stage 接入限流器：process 执行次数不超过令牌供给。
func TestRateLimiterStage(t *testing.T) {
	// 限流 20/s、桶 10，跑 200ms，process 执行数应受到严格限制。
	rl := NewRateLimiter(20, 10)
	in := make(chan int, 1000)
	var processed int64
	var mu sync.Mutex

	s := NewStage("rl", StageConfig{Workers: 4, OutCap: 1000, RateLimiter: rl}, in, nil,
		func(ctx context.Context, x int) (int, error) {
			mu.Lock()
			processed++
			mu.Unlock()
			return x, nil
		})
	ctx, cancel := context.WithCancel(context.Background())
	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 灌入 500 条。
	go func() {
		for i := 0; i < 500; i++ {
			in <- i
		}
		close(in)
	}()

	// 退出前消费 Stage 输出（同包访问 s.output，勿自建通道）。
	done := make(chan struct{})
	go func() {
		for range s.output {
		}
		close(done)
	}()

	time.Sleep(300 * time.Millisecond)
	cancel()
	s.Close(time.Second)

	// 20/s 限流，300ms 最多约 10(桶) + 6 ≈ 16，绝不该到 500。
	mu.Lock()
	count := processed
	mu.Unlock()
	if count > 100 {
		t.Errorf("限流失效：300ms 内处理了 %d 条（预期 ≤ ~16）", count)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("output 未排空")
	}
}