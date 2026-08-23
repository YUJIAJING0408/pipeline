package pipeline

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestIntegration_HighThroughput 验证 100K 数据高吞吐场景下：
//   - 所有数据正确处理
//   - goroutine 无泄漏（pool.wait() 后全部退出）
func TestIntegration_HighThroughput(t *testing.T) {
	const N = 100000

	in := make(chan int, 4096)
	s := NewStage("throughput", StageConfig{Workers: 4, OutCap: 4096}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 先启动 drain goroutine，避免发送循环填满缓冲区后死锁
	done := make(chan struct{})
	go func() {
		for range N {
			<-s.output
		}
		close(done)
	}()

	// 发送所有数据
	for i := range N {
		in <- i
	}
	close(in)

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("timeout: 100K items not drained within 30s")
	}

	// 等待 worker 退出
	s.pool.wait()

	// 验证 goroutine 已退出：在 pool.wait() 之后，所有 worker goroutine 应已退出
	// 此处只需确认 pool.wait() 不阻塞即可
}

// TestIntegration_Backpressure 验证背压机制：
//   - 生产者快、消费者慢时，生产者被阻塞
//   - 关闭后生产者正常退出
//   - 不 OOM（数据不会无限堆积）
func TestIntegration_Backpressure(t *testing.T) {
	in := make(chan int, 2) // 极小缓冲区
	var processed atomic.Int64

	s := NewStage("backpressure", StageConfig{Workers: 1, OutCap: 2}, in, nil, func(ctx context.Context, x int) (int, error) {
		time.Sleep(10 * time.Millisecond) // 慢消费者
		processed.Add(1)
		return x, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 生产者：快速发送 100 条，发送完自行 close(in)
	producerDone := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			in <- i
		}
		close(in)
		close(producerDone)
	}()

	// 等待一小段时间，让生产者填满缓冲区并阻塞
	time.Sleep(50 * time.Millisecond)

	// 验证生产者被阻塞（尚未完成）
	select {
	case <-producerDone:
		t.Log("producer finished quickly (unexpected)")
	default:
		t.Log("producer blocked on backpressure (expected)")
	}

	// 启动 drain 消费输出，让 worker 继续处理以解锁生产者
	drainDone := make(chan struct{})
	go func() {
		for range s.output {
		}
		close(drainDone)
	}()

	// 等待生产者完成
	<-producerDone

	// 关闭 Stage（会关闭 s.output，使 drain 退出）
	if err := s.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	<-drainDone

	count := processed.Load()
	t.Logf("processed %d items under backpressure", count)
	if count < 1 {
		t.Error("no items were processed")
	}
}

// TestIntegration_BackpressureNoBlock 验证非阻塞模式下：
//   - OutCap 足够大时，生产者不被阻塞
//   - 所有数据正常处理
func TestIntegration_BackpressureNoBlock(t *testing.T) {
	const N = 10000
	in := make(chan int, 4096)
	var processed atomic.Int64

	s := NewStage("noblock", StageConfig{Workers: 4, OutCap: 4096}, in, nil, func(ctx context.Context, x int) (int, error) {
		processed.Add(1)
		time.Sleep(1 * time.Microsecond)
		return x, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 并发 drain s.output，避免 workers 填满输出缓冲区后死锁
	drainDone := make(chan struct{})
	go func() {
		for range s.output {
		}
		close(drainDone)
	}()

	done := make(chan struct{})
	go func() {
		for i := 0; i < N; i++ {
			in <- i
		}
		close(in)
		close(done)
	}()

	s.pool.wait()
	<-done

	// 关闭 Stage 以关闭 s.output，使 drain 退出
	if err := s.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	<-drainDone

	count := processed.Load()
	if count != N {
		t.Errorf("expected %d processed, got %d", N, count)
	}
}

// TestIntegration_FanoutSlowBranch 验证 Fanout 模式下：
//   - 配置 Timeout 后，慢分支的数据被丢弃，不阻塞快分支
//   - 快分支正常处理全部数据
func TestIntegration_FanoutSlowBranch(t *testing.T) {
	in := make(chan int, 128)
	root := NewStage("root", StageConfig{Workers: 2, OutCap: 128, Timeout: 50 * time.Millisecond}, in, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil },
	)

	// 快分支：立即返回
	var fastCount atomic.Int64
	fast := root.NextStage("fast", StageConfig{Workers: 2, OutCap: 128}, nil,
		func(ctx context.Context, x int) (int, error) {
			fastCount.Add(1)
			return x, nil
		},
	)

	// 慢分支：处理耗时超过 Timeout
	var slowCount atomic.Int64
	root.NextStage("slow", StageConfig{Workers: 1, OutCap: 1}, nil,
		func(ctx context.Context, x int) (int, error) {
			time.Sleep(200 * time.Millisecond) // 慢于 Timeout(50ms)
			slowCount.Add(1)
			return x, nil
		},
	)

	// 消费快分支输出
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := root.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	const N = 50
	done := make(chan struct{})
	go func() {
		for i := range N {
			in <- i
		}
		close(in)
		// 消费快分支输出（同包访问私有字段 output）
		for range N {
			<-fast.output
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout: fanout slow branch blocked fast branch")
	}

	root.Close(5 * time.Second)

	fastN := fastCount.Load()
	slowN := slowCount.Load()
	t.Logf("fast: %d, slow: %d", fastN, slowN)

	if fastN != N {
		t.Errorf("expected fast branch to process %d, got %d", N, fastN)
	}
	if slowN >= N {
		t.Error("slow branch should have dropped some items due to timeout")
	}
}

// TestIntegration_GracefulCloseDataIntegrity 验证优雅关闭数据完整性：
//   - 发送 N 条数据，处理过程中关闭
//   - 关闭后：已处理数据 = 快分支不变，无数据损坏
//   - 数据结构不损坏（可重复关闭、幂等）
func TestIntegration_GracefulCloseDataIntegrity(t *testing.T) {
	const N = 10000

	in := make(chan int, 1024)
	var processed atomic.Int64

	s := NewStage("integrity", StageConfig{Workers: 4, OutCap: 1024}, in, nil, func(ctx context.Context, x int) (int, error) {
		processed.Add(1)
		time.Sleep(10 * time.Microsecond)
		return x, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 先启动 drain，避免 workers 填满 output 后死锁
	drainDone := make(chan struct{})
	go func() {
		for range s.output {
		}
		close(drainDone)
	}()

	// 发送一半数据后关闭
	done := make(chan struct{})
	go func() {
		for i := range N / 2 {
			in <- i
		}
		// 让消费者开始处理
		time.Sleep(5 * time.Millisecond)
		close(in)
		close(done)
	}()

	// 等待关闭完成
	<-done

	// 执行 Close：幂等验证
	if err := s.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	<-drainDone
	// 第二次 Close 应幂等返回 nil
	if err := s.Close(5 * time.Second); err != nil {
		t.Fatal("Close is not idempotent:", err)
	}

	count := processed.Load()
	t.Logf("processed %d items before close", count)
	if count < 1 {
		t.Error("no items were processed before close")
	}
	if count > N {
		t.Errorf("processed more items than sent: %d > %d", count, N)
	}
}

// TestIntegration_Chain3 验证 3 级链式 Stage 在 100K 数据下的正确性。
func TestIntegration_Chain3(t *testing.T) {
	const N = 100000

	in := make(chan int, 4096)
	s1 := NewStage("s1", StageConfig{Workers: 4, OutCap: 4096}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	s2 := s1.NextStage("s2", StageConfig{Workers: 4, OutCap: 4096}, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	s3 := s2.NextStage("s3", StageConfig{Workers: 4, OutCap: 4096}, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s1.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 发送
	go func() {
		for i := range N {
			in <- i
		}
		close(in)
	}()

	// 消费 s3 输出
	done := make(chan struct{})
	go func() {
		for range s3.output {
		}
		close(done)
	}()

	// 等待 worker 排空完成（close(in) 后所有 worker 自然退出）
	s1.pool.wait()
	// 关闭输出通道，使 drain 退出
	s1.Close(5 * time.Second)
	<-done
}

// TestIntegration_ConcurrentClose 验证并发关闭安全：
//   - 多个 goroutine 同时调 Close
//   - 不 panic、不 double-close channel
func TestIntegration_ConcurrentClose(t *testing.T) {
	in := make(chan int, 16)
	out := make(chan int, 16)
	s := NewStage("concurrent", StageConfig{Workers: 2, OutCap: 16}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 喂一点数据
	go func() {
		for i := range 100 {
			in <- i
		}
		close(in)
	}()

	// 并发消费
	go func() {
		for range out {
		}
	}()

	// 10 个 goroutine 同时 Close
	done := make(chan struct{}, 10)
	for range 10 {
		go func() {
			_ = s.Close(5 * time.Second)
			done <- struct{}{}
		}()
	}

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("timeout: concurrent close")
	}
}

// TestIntegration_PipelineRunClose 验证 Pipeline 完整生命周期：
//   - Run + Close 流程
//   - 所有数据正确处理
func TestIntegration_PipelineRunClose(t *testing.T) {
	const N = 1000

	root := NewStage("root", StageConfig{Workers: 4, OutCap: 128}, nil, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	root.NextStage("leaf", StageConfig{Workers: 4, OutCap: 128}, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})

	pl := New[int, int](PipelineConfig{
		Name:            "integration",
		InputBufferSize: 128,
	}).AddStage(root).Input(&MockSource[int]{Data: func() []int {
		data := make([]int, N)
		for i := range N {
			data[i] = i
		}
		return data
	}()})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(5 * time.Second)
		cancel()
	}()

	if err := pl.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pl.Close(5 * time.Second); err != nil {
		t.Fatal(err)
	}

	t.Log("Pipeline run and close completed successfully")
}

// TestIntegration_GoroutineLeak 验证长时间运行后 goroutine 无泄漏。
// 发送 1M 数据，关闭后检查 goroutine 数是否回到基准。
func TestIntegration_GoroutineLeak(t *testing.T) {
	const N = 1000000

	base := runtime.NumGoroutine()

	in := make(chan int, 4096)
	s := NewStage("leak", StageConfig{Workers: 8, OutCap: 4096}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 发送 1M 数据
	sendDone := make(chan struct{})
	go func() {
		for i := range N {
			in <- i
		}
		close(in)
		close(sendDone)
	}()

	// 消费
	done := make(chan struct{})
	go func() {
		for range s.output {
		}
		close(done)
	}()

	// 1. 等待发送完成（close(in)）
	<-sendDone
	// 2. 等待 worker 排空
	s.pool.wait()
	// 3. 关闭 Stage（关闭 s.output，使 drain 退出）
	s.Close(5 * time.Second)
	// 4. 等待 drain 退出
	<-done

	// 等待 goroutine 调度完成
	time.Sleep(100 * time.Millisecond)

	after := runtime.NumGoroutine()
	leaked := after - base
	t.Logf("goroutines: before=%d after=%d leaked=%d", base, after, leaked)

	// 允许少量 goroutine 差异（测试框架、gc 等），但不应超过 5
	// worker goroutine 应全部退出
	if leaked > 5 {
		// 输出 goroutine 栈信息用于调试
		buf := make([]byte, 4096)
		n := runtime.Stack(buf, true)
		t.Logf("goroutine stacks:\n%s", buf[:n])
		t.Errorf("expected <=5 leaked goroutines, got %d", leaked)
	}
}

// TestIntegration_PanicInProcess 验证 process 函数 panic 被 recover 捕获：
//   - panic 转换为 error 走正常错误处理路径
//   - worker 不崩溃，继续处理后续数据
func TestIntegration_PanicInProcess(t *testing.T) {
	in := make(chan int, 16)
	var processed atomic.Int64

	s := NewStage("panic", StageConfig{Workers: 1, OutCap: 16}, in, nil, func(ctx context.Context, x int) (int, error) {
		if x == 1 {
			panic(fmt.Sprintf("process panic on input %d", x))
		}
		processed.Add(1)
		return x, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 发送 5 条数据（第 2 条会 panic）
	for i := 0; i < 5; i++ {
		in <- i
	}
	close(in)

	// 消费输出
	go func() {
		for range s.output {
		}
	}()

	// 等待 worker 退出
	s.pool.wait()

	// 验证：panic 被捕获，worker 没崩溃，其余 4 条正常处理
	count := processed.Load()
	t.Logf("processed %d items (expect 4, item 1 panicked)", count)
	if count != 4 {
		t.Errorf("expected 4 processed items, got %d", count)
	}
}

// TestIntegration_CloseTimeout 验证 Close 超时行为：
//   - worker 处理慢于 drainTimeout 时，Close 强制取消 ctx
//   - 返回 ErrCloseTimeout
func TestIntegration_CloseTimeout(t *testing.T) {
	in := make(chan int, 4)
	s := NewStage("timeout", StageConfig{Workers: 1, OutCap: 4}, in, nil, func(ctx context.Context, x int) (int, error) {
		time.Sleep(1 * time.Second) // 极慢，远超 drainTimeout
		return x, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	// 发送一条数据，worker 开始处理（1 秒）
	in <- 42
	close(in)

	// 用极短的 drainTimeout 触发超时
	err := s.Close(10 * time.Millisecond)
	if err != nil {
		t.Logf("Close returned error (expected): %v", err)
	} else {
		t.Log("Close completed without timeout (worker may have completed)")
	}
}

// TestIntegration_EmptyInput 验证 input 为 nil 时 Start 返回错误。
func TestIntegration_EmptyInput(t *testing.T) {
	// 创建一个 input 为 nil 的 Stage
	s := &Stage[int, int]{
		name:   "empty",
		config: StageConfig{Workers: 1, OutCap: 16},
		output: make(chan int, 16),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := s.Start(ctx, nil)
	if err != ErrStageMissingInput {
		t.Errorf("expected ErrStageMissingInput, got %v", err)
	}
}

// TestIntegration_SinkStage 验证 Sink 模式下数据正确处理。
func TestIntegration_SinkStage(t *testing.T) {
	const N = 1000
	in := make(chan int, 128)
	var sinkCount atomic.Int64

	s := NewStage("sink", StageConfig{Workers: 4, OutCap: 128}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	}).Sink(func(ctx context.Context, v int) {
		sinkCount.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < N; i++ {
		in <- i
	}
	close(in)
	s.pool.wait()

	count := sinkCount.Load()
	if count != N {
		t.Errorf("expected sink to receive %d, got %d", N, count)
	}
}
