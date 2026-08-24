package pipeline

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestWorkerPoolProcess 验证工作池并发消费 in、结果写入 out。
func TestWorkerPoolProcess(t *testing.T) {
	in := make(chan int, 10)
	out := make(chan int, 10)
	wp := newWorkerPool(3, in, out, func(ctx context.Context, x int) (int, error) {
		return x * 2, nil
	}, 0, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	for i := 1; i <= 6; i++ {
		in <- i
	}
	close(in)
	wp.wait()

	got := map[int]bool{}
	for i := 1; i <= 6; i++ {
		got[<-out] = true
	}
	for i := 1; i <= 6; i++ {
		if !got[i*2] {
			t.Errorf("输出缺少 %d", i*2)
		}
	}
}

// TestWorkerPoolErrorSkip 验证处理出错时 onError 回调触发且该条不写入输出。
func TestWorkerPoolErrorSkip(t *testing.T) {
	in := make(chan int, 2)
	out := make(chan int, 2)
	var errCount atomic.Int64
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		if x%2 == 0 {
			return 0, context.DeadlineExceeded
		}
		return x, nil
	}, 0, func(err error, in int) {
		errCount.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 1
	in <- 2
	in <- 3
	close(in)
	wp.wait()

	if got := errCount.Load(); got != 1 {
		t.Errorf("onError 调用次数 = %d, want 1", got)
	}
	// 只应有 1 与 3 的输出（2 被跳过）。
	got := map[int]bool{<-out: true, <-out: true}
	if !got[1] || !got[3] {
		t.Errorf("输出应为 {1,3}, got %v", got)
	}
}

// TestWorkerPoolTimeout 验证单条处理超时触发 onError（ctx 超时取消）。
func TestWorkerPoolTimeout(t *testing.T) {
	in := make(chan int, 1)
	out := make(chan int, 1)
	var errCount atomic.Int64
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return x, nil
		}
	}, 30*time.Millisecond, func(err error, in int) {
		errCount.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 1
	close(in)
	wp.wait()

	if got := errCount.Load(); got != 1 {
		t.Errorf("超时应触发 onError, got count=%d want 1", got)
	}
	select {
	case <-out:
		t.Error("超时的数据不应写入输出")
	default:
	}
}

// TestWorkerPoolHooks 验证生命周期钩子在 process 前后正确触发。
func TestWorkerPoolHooks(t *testing.T) {
	in := make(chan int, 2)
	out := make(chan int, 2)

	var mu sync.Mutex
	var calls []struct {
		in  int
		out int
		err error
	}

	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		if x < 0 {
			return 0, errors.New("negative")
		}
		return x * 10, nil
	}, 0, nil)
	wp.stageName = "hooks"
	wp.hooks = &StageHooks{
		OnBeforeProcess: func(ctx context.Context, in any) context.Context {
			return ctx
		},
		OnAfterProcess: func(ctx context.Context, in any, out any, err error, latency time.Duration) {
			mu.Lock()
			defer mu.Unlock()
			o, _ := out.(int)
			calls = append(calls, struct {
				in  int
				out int
				err error
			}{in: in.(int), out: o, err: err})
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 7
	in <- -3
	close(in)
	wp.wait()

	mu.Lock()
	count := len(calls)
	first := calls[0]
	second := calls[1]
	mu.Unlock()

	if count != 2 {
		t.Fatalf("OnAfterProcess 调用次数 = %d, want 2", count)
	}
	if first.in != 7 || first.out != 70 || first.err != nil {
		t.Errorf("成功数据: in=%d out=%d err=%v, want 7/70/nil", first.in, first.out, first.err)
	}
	if second.in != -3 || second.out != 0 || second.err == nil {
		t.Errorf("失败数据: in=%d out=%d err=%v, want -3/0/非nil", second.in, second.out, second.err)
	}
	// 消费输出。
	<-out // 7
	// -3 不输出
}

// TestWorkerPoolHooksNil 验证 nil 钩子不产生额外开销。
func TestWorkerPoolHooksNil(t *testing.T) {
	in := make(chan int, 1)
	out := make(chan int, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		return x, nil
	}, 0, nil)
	wp.hooks = nil

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)
	in <- 99
	close(in)
	wp.wait()
	if v := <-out; v != 99 {
		t.Errorf("nil hooks 输出 = %d, want 99", v)
	}
}
func TestWorkerPoolCtxCancel(t *testing.T) {
	in := make(chan int)  // 无缓冲
	out := make(chan int) // 无缓冲：若无消费者且 ctx 不取消会阻塞
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		return x, nil
	}, 0, nil)

	ctx, cancel := context.WithCancel(context.Background())
	wp.start(ctx)

	in <- 1 // worker 消费此条（可能已完成处理）
	cancel()
	wp.wait()

	// ctx 取消后 worker 已退出：无缓冲发送不再有接收方，必走 default 分支。
	select {
	case in <- 2:
		t.Error("ctx 取消后 worker 不应再接收数据")
	default:
	}
}

// captureStdout 临时捕获进程 stdout，返回读取函数（测试用）。
func captureStdout(t *testing.T) func() string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w

	done := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		n, _ := r.Read(buf)
		done <- string(buf[:n])
	}()

	return func() string {
		_ = w.Close()
		os.Stdout = old
		return <-done
	}
}

// TestWorkerPoolSlowLog 验证慢处理超过阈值打印慢日志（内容含 stage 名）。
func TestWorkerPoolSlowLog(t *testing.T) {
	in := make(chan int, 1)
	out := make(chan int, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		time.Sleep(50 * time.Millisecond) // 慢处理
		return x, nil
	}, 0, nil)
	wp.stageName = "slow-stage"
	wp.slowThreshold = 20 * time.Millisecond

	capture := captureStdout(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 1
	close(in)
	wp.wait()

	got := capture()
	if !strings.Contains(got, "[slow]") || !strings.Contains(got, "slow-stage") {
		t.Errorf("应打印慢日志, got: %q", got)
	}
}

// TestWorkerPoolNoSlowLog 验证未超过阈值不打印慢日志。
func TestWorkerPoolNoSlowLog(t *testing.T) {
	in := make(chan int, 1)
	out := make(chan int, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		time.Sleep(5 * time.Millisecond) // 快速处理
		return x, nil
	}, 0, nil)
	wp.stageName = "fast-stage"
	wp.slowThreshold = 100 * time.Millisecond

	capture := captureStdout(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 1
	close(in)
	wp.wait()

	got := capture()
	if strings.Contains(got, "[slow]") {
		t.Errorf("未超阈值不应打印慢日志, got: %q", got)
	}
}

// TestWorkerPoolSlowThresholdZero 验证阈值 0（不启用）时即使慢也不打印。
func TestWorkerPoolSlowThresholdZero(t *testing.T) {
	in := make(chan int, 1)
	out := make(chan int, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		time.Sleep(50 * time.Millisecond)
		return x, nil
	}, 0, nil)
	wp.stageName = "no-threshold"
	wp.slowThreshold = 0 // 不启用

	capture := captureStdout(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)

	in <- 1
	close(in)
	wp.wait()

	got := capture()
	if strings.Contains(got, "[slow]") {
		t.Errorf("阈值 0 不应打印慢日志, got: %q", got)
	}
}

// TestWorkerPoolRetryCloseDeadLetter 验证关闭瞬间重试中断也补投死信（评审 #2）。
func TestWorkerPoolRetryCloseDeadLetter(t *testing.T) {
	sink := &memSink{}
	in := make(chan int, 1)
	out := make(chan int, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		return 0, errors.New("always fail")
	}, 0, nil)
	wp.stageName = "retry-close-dl"
	wp.errPol = &ErrPolicy{
		Mode:       ErrModeRetryFallback,
		MaxRetry:   3,
		RetryDelay: time.Hour, // 重试间隔超长：必然在等待中被 ctx 取消打断
	}
	wp.dlWriter = &deadLetterWriter{sink: sink, marshal: nil}

	ctx, cancel := context.WithCancel(context.Background())
	wp.start(ctx)
	in <- 1
	close(in)
	// 立即取消 ctx（不等重试 timer），验证中断路径补投死信。
	time.Sleep(20 * time.Millisecond)
	cancel()
	wp.wait()

	recs := sink.records()
	if len(recs) != 1 {
		t.Fatalf("关闭瞬间重试中断应补投 1 条死信, got %d", len(recs))
	}
	if recs[0].Retried != 0 {
		t.Errorf("Retried = %d, want 0（等待首个重试间隔前即中断）", recs[0].Retried)
	}
}
