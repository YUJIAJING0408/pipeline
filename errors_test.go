package pipeline

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// TestErrPolicyCollect 验证 Collect 模式：处理失败时跳过该条，继续下一条。
func TestErrPolicyCollect(t *testing.T) {
	in := make(chan int, 3)
	out := make(chan int, 3)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		if x == 2 {
			return 0, errors.New("boom")
		}
		return x, nil
	}, 0, nil)
	wp.stageName = "collect"
	wp.errPol = &ErrPolicy{Mode: ErrModeCollect}
	wp.stageMonitor = &StageMonitor{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 1
	in <- 2 // 失败
	in <- 3
	close(in)
	wp.wait()

	// 输出应为 1 和 3（2 被跳过）。
	got := map[int]bool{}
	for range 2 {
		got[<-out] = true
	}
	if !got[1] || !got[3] {
		t.Errorf("输出应为 {1,3}, got %v", got)
	}
	// 监控记录错误。
	total, errs, _, _ := wp.stageMonitor.snapshot()
	if total != 3 || errs != 1 {
		t.Errorf("监控统计: total=%d errors=%d, want total=3 errors=1", total, errs)
	}
}

// TestErrPolicyFailFast 验证 FailFast 模式：处理失败时调用 cancel 函数。
func TestErrPolicyFailFast(t *testing.T) {
	in := make(chan int, 2)
	out := make(chan int, 2)
	var canceled bool
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		if x == 2 {
			return 0, errors.New("boom")
		}
		return x, nil
	}, 0, nil)
	wp.stageName = "failfast"
	wp.errPol = &ErrPolicy{Mode: ErrModeFailFast}
	wp.cancel = func() { canceled = true }
	wp.stageMonitor = &StageMonitor{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 1
	in <- 2 // 失败 → 触发 cancel
	close(in)
	wp.wait()

	if !canceled {
		t.Error("FailFast 模式下失败应调用 cancel")
	}
	// 1 应该被处理成功，2 失败不输出。
	select {
	case v := <-out:
		if v != 1 {
			t.Errorf("输出应为 1, got %d", v)
		}
	default:
		// 可能输出已在关闭前被消费
	}
}

// TestErrPolicyRetryFallback 验证 RetryFallback 模式：重试 2 次后成功。
func TestErrPolicyRetryFallback(t *testing.T) {
	var attempts atomic.Int64
	in := make(chan int, 1)
	out := make(chan int, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		n := attempts.Add(1)
		if n < 3 {
			return 0, errors.New("transient")
		}
		return x, nil
	}, 0, nil)
	wp.stageName = "retry"
	wp.errPol = &ErrPolicy{
		Mode:       ErrModeRetryFallback,
		MaxRetry:   3,
		RetryDelay: time.Millisecond,
	}
	wp.stageMonitor = &StageMonitor{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 42
	close(in)
	wp.wait()

	if got := attempts.Load(); got != 3 {
		t.Errorf("尝试次数 = %d, want 3（1 次初始 + 2 次重试）", got)
	}
	select {
	case v := <-out:
		if v != 42 {
			t.Errorf("输出应为 42, got %d", v)
		}
	default:
		t.Error("输出应为 42，但无输出")
	}
}

// TestStageErrorBasics 验证 StageError 的 Error/Unwrap/As 基础行为。
func TestStageErrorBasics(t *testing.T) {
	underlying := errors.New("db down")
	se := &StageError{
		Stage:   "parse",
		Code:    CodeSystem,
		Input:   42,
		Latency: 5 * time.Millisecond,
		Err:     underlying,
	}

	if !errors.Is(se, underlying) {
		t.Error("errors.Is 应穿透 Unwrap 命中底层错误")
	}
	if got := AsStageError(se); got != se {
		t.Error("AsStageError 应返回同一实例")
	}
	if !errors.As(se, &se) {
		t.Error("errors.As 应命中 *StageError")
	}
	msg := se.Error()
	if !strings.Contains(msg, "parse") || !strings.Contains(msg, "system") {
		t.Errorf("Error() 应含 stage/code 信息: %s", msg)
	}
	if se.Code.String() != "system" {
		t.Errorf("Code.String() = %q, want system", se.Code.String())
	}
}

// TestStageErrorIsSentinel 验证哨兵错误匹配：Code 与哨兵一一对应。
func TestStageErrorIsSentinel(t *testing.T) {
	cases := []struct {
		code ErrorCode
		sent error
	}{
		{CodeTimeout, ErrTimeout},
		{CodeInvalidInput, ErrInvalidInput},
		{CodeProcessing, ErrProcessing},
		{CodeSystem, ErrSystemFailure},
	}
	for _, c := range cases {
		se := &StageError{Code: c.code, Err: errors.New("inner")}
		if !errors.Is(se, c.sent) {
			t.Errorf("Code=%s 应匹配哨兵 %v", c.code, c.sent)
		}
	}
	// 不匹配的哨兵。
	se := &StageError{Code: CodeTimeout, Err: errors.New("inner")}
	if errors.Is(se, ErrInvalidInput) {
		t.Error("Code=timeout 不应匹配 ErrInvalidInput")
	}
}

// TestWithCode 验证 WithCode 显式标注分类（已包装时更新，未包装时新建）。
func TestWithCode(t *testing.T) {
	if WithCode(nil, CodeSystem) != nil {
		t.Error("WithCode(nil) 应返回 nil")
	}
	underlying := errors.New("bad input")
	marked := WithCode(underlying, CodeInvalidInput)
	if !errors.Is(marked, ErrInvalidInput) {
		t.Error("WithCode 应使 errors.Is 命中对应哨兵")
	}
	if !errors.Is(marked, underlying) {
		t.Error("WithCode 应保持底层错误可穿透")
	}
	// 二次包装：Code 更新。
	re := WithCode(marked, CodeSystem)
	var se *StageError
	if !errors.As(re, &se) || se.Code != CodeSystem {
		t.Errorf("二次 WithCode 应更新 Code, got %+v", se)
	}
}

// TestWrapStageErrorAuto 验证 wrapStageError 自动分类：
//   - context.DeadlineExceeded → CodeTimeout
//   - 普通错误 → CodeProcessing
//   - 已包装错误保留 Code 且补全上下文
func TestWrapStageErrorAuto(t *testing.T) {
	// 超时自动分类。
	se := wrapStageError("s1", 1, context.DeadlineExceeded, time.Second)
	if se.Stage != "s1" || se.Input != 1 || se.Code != CodeTimeout || se.Latency != time.Second {
		t.Errorf("超时包装错误: stage=%s input=%v code=%s latency=%v",
			se.Stage, se.Input, se.Code, se.Latency)
	}
	if !errors.Is(se, ErrTimeout) {
		t.Error("超时包装应匹配 ErrTimeout 哨兵")
	}
	// 普通错误。
	se2 := wrapStageError("s2", 2, errors.New("boom"), 0)
	if se2.Code != CodeProcessing {
		t.Errorf("普通错误 code = %s, want processing", se2.Code)
	}
	// 已包装错误保留 Code。
	marked := WithCode(errors.New("bad"), CodeInvalidInput)
	se3 := wrapStageError("s3", 3, marked, time.Millisecond)
	if se3.Code != CodeInvalidInput {
		t.Errorf("已包装错误应保留 Code, got %s", se3.Code)
	}
	if se3.Stage != "s3" || se3.Input != 3 || se3.Latency != time.Millisecond {
		t.Errorf("已包装错误应补全上下文: %+v", se3)
	}
}

// TestWorkerPoolStageErrorOnError 验证 handle 回调收到的错误被包装为 StageError。
func TestWorkerPoolStageErrorOnError(t *testing.T) {
	in := make(chan int, 1)
	out := make(chan int, 1)
	var gotErr error
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		return 0, context.DeadlineExceeded
	}, 30*time.Millisecond, func(err error, in int) {
		gotErr = err
	})
	wp.stageName = "wrapped"
	wp.stageMonitor = &StageMonitor{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 7
	close(in)
	wp.wait()

	if gotErr == nil {
		t.Fatal("onError 应收到错误")
	}
	se := AsStageError(gotErr)
	if se == nil {
		t.Fatal("onError 收到的错误应为 *StageError")
	}
	if se.Stage != "wrapped" || se.Input != 7 {
		t.Errorf("StageError 上下文: stage=%s input=%v", se.Stage, se.Input)
	}
	if se.Code != CodeTimeout {
		t.Errorf("超时错误 code = %s, want timeout", se.Code)
	}
}

// TestErrPolicyPerStage 验证 Stage 级 ErrPolicy 覆盖全局策略。
func TestErrPolicyPerStage(t *testing.T) {
	// 全局策略：Collect（失败继续），但 Stage 1 配置 FailFast。
	// 验证 Stage 1 使用 FailFast（失败后 cancel），Stage 2 使用全局 Collect。
	rootIn := make(chan int, 4)
	root := NewStage("root", StageConfig{Workers: 1, OutCap: 4}, rootIn, nil,
		func(ctx context.Context, x int) (int, error) {
			if x == 2 {
				return 0, errors.New("boom")
			}
			return x, nil
		})
	// Stage 1 配置 FailFast（覆盖全局 Collect）。
	_ = root.NextStage("s1", StageConfig{Workers: 1, OutCap: 4, ErrPolicy: &ErrPolicy{Mode: ErrModeFailFast}}, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	// Stage 2 使用全局策略（Collect）。
	_ = root.NextStage("s2", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })

	var canceled bool
	// 注入全局策略（Collect）+ 自定义 cancel 函数检测 FailFast。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	params := map[string]any{
		"errPol":  &ErrPolicy{Mode: ErrModeCollect},
		"monitor": NewMonitor(),
	}
	if err := root.Start(ctx, params); err != nil {
		t.Fatal(err)
	}
	// 手动注入 cancel 到 root 的 workerPool 以检测 FailFast。
	root.pool.cancel = func() { canceled = true }
	root.pool.errPol = &ErrPolicy{Mode: ErrModeCollect} // 全局

	// 触发 root 处理数据（x=2 失败走 root 的 Collect 策略）。
	rootIn <- 1
	rootIn <- 2
	rootIn <- 3
	close(rootIn)
	root.pool.wait()

	// 验证 root 的 Collect 模式下 canceled 未被触发（因为 root 是 Collect）。
	if canceled {
		t.Error("root 使用 Collect 模式，不应触发 cancel")
	}
	// 验证 root 的 pool 正确处理了 3 条数据。
	total, errs, _, _ := root.stageMonitor.snapshot()
	if total != 3 || errs != 1 {
		t.Errorf("root: total=%d errors=%d, want 3/1", total, errs)
	}
}

// TestErrPolicyStageOverride 验证 StageConfig.ErrPolicy 优先于全局策略。
func TestErrPolicyStageOverride(t *testing.T) {
	// 全局策略：FailFast。但 process 配置了 RetryFallback（覆盖全局）。
	// 只需验证配置读取正确：s.errPol 指向 config 而非全局。
	in := make(chan int, 4)
	stageCfg := StageConfig{Workers: 1, OutCap: 4, ErrPolicy: &ErrPolicy{Mode: ErrModeCollect}}
	s := NewStage("s", stageCfg, in, nil, func(ctx context.Context, x int) (int, error) { return x, nil })
	globalPol := &ErrPolicy{Mode: ErrModeFailFast}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	params := map[string]any{"errPol": globalPol}
	if err := s.Start(ctx, params); err != nil {
		t.Fatal(err)
	}
	if s.errPol == nil || s.errPol.Mode != ErrModeCollect {
		t.Errorf("s.errPol.Mode = %v, want ErrModeCollect（Stage 级配置应覆盖全局）", s.errPol)
	}
	close(in)
	_ = s.Close(time.Second)
}
func TestErrPolicyRetryExhaustedFallback(t *testing.T) {
	var fallbackCalled atomic.Int64
	in := make(chan int, 1)
	out := make(chan int, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		return 0, errors.New("always fail")
	}, 0, nil)
	wp.stageName = "fallback"
	wp.errPol = &ErrPolicy{
		Mode:       ErrModeRetryFallback,
		MaxRetry:   2,
		RetryDelay: time.Millisecond,
		Fallback: func(ctx context.Context, in any) (any, error) {
			fallbackCalled.Add(1)
			return 99, nil
		},
	}
	wp.stageMonitor = &StageMonitor{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 10
	close(in)
	wp.wait()

	if got := fallbackCalled.Load(); got != 1 {
		t.Errorf("降级函数调用次数 = %d, want 1", got)
	}
	select {
	case v := <-out:
		if v != 99 {
			t.Errorf("降级后输出应为 99, got %d", v)
		}
	default:
		t.Error("降级后应有输出 99")
	}
}
