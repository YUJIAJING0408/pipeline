package pipeline

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestNewPipeline 验证 Builder 组装能力与 D-18 配置存取。
func TestNewPipeline(t *testing.T) {
	p := New[string, string](PipelineConfig{
		Name:            "test",
		InputBufferSize: 64,
		LogDir:          "./logs-x",
		LogEnabled:      true,
	}).
		AddStage(newPassThrough("s1")).
		Input(&MockSource[string]{Data: []string{"a"}}).
		LogDir("./override-logs")

	if p.stage == nil {
		t.Fatal("stage 未设置")
	}
	if got := len(p.sources); got != 1 {
		t.Errorf("sources 数量 = %d, want 1", got)
	}
	if p.config.Name != "test" || p.config.InputBufferSize != 64 || p.config.LogEnabled != true {
		t.Errorf("配置未正确保存: %+v", p.config)
	}
	if p.config.LogDir != "./override-logs" {
		t.Errorf("LogDir 覆盖后 = %q, want ./override-logs", p.config.LogDir)
	}
}

// TestNewPipelineDefaultLogDir 验证 LogDir 为空时默认 ./logs（D-18）。
func TestNewPipelineDefaultLogDir(t *testing.T) {
	p := New[string, string](PipelineConfig{})
	if p.config.LogDir != "./logs" {
		t.Errorf("默认 LogDir = %q, want ./logs", p.config.LogDir)
	}
}

// TestPipelineRunEndToEnd 验证完整链路：MockSource 注入 → NextStage 链式 → 末节点输出。
func TestPipelineRunEndToEnd(t *testing.T) {
	s1 := NewStage("upper", StageConfig{Workers: 2, OutCap: 8}, nil, nil, func(ctx context.Context, in string) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			return in + "+up", nil
		}
	})
	s2 := s1.NextStage("exclaim", StageConfig{Workers: 2, OutCap: 8}, nil, func(ctx context.Context, in string) (string, error) {
		return in + "!", nil
	})

	p := New[string, string](PipelineConfig{InputBufferSize: 8}).
		AddStage(s1).
		Input(&MockSource[string]{Data: []string{"a", "b"}})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// 并发消费末节点输出（s2.output 是 s1.NextStage 的产物）。
	got := make(map[string]bool)
	readDone := make(chan struct{})
	go func() {
		for range 2 {
			got[<-s2.output] = true
		}
		close(readDone)
	}()

	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		t.Fatalf("链路未在超时内产出 2 条结果, got=%v", got)
	}
	if !got["a+up!"] || !got["b+up!"] {
		t.Errorf("输出应为 {a+up!, b+up!}, got %v", got)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run 返回错误: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run 未在 ctx 取消后返回")
	}

	if err := p.Close(time.Second); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := p.Close(time.Second); err != nil {
		t.Errorf("重复 Close 应返回 nil, got %v", err)
	}
}

// TestPipelineRunTwiceRejected 验证 Run 不可重复调用。
func TestPipelineRunTwiceRejected(t *testing.T) {
	s1 := newPassThrough("s1")
	p := New[string, string](PipelineConfig{}).
		AddStage(s1).
		Input(&MockSource[string]{Data: []string{"a"}})

	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-time.After(50 * time.Millisecond); cancel() }()

	if err := p.Run(ctx); err != nil {
		t.Fatalf("首次 Run 失败: %v", err)
	}
	if err := p.Run(context.Background()); err != ErrPipelineStarted {
		t.Errorf("二次 Run 错误 = %v, want %v", err, ErrPipelineStarted)
	}
}

// TestPipelineGraphTD 验证 GraphTD 输出正确可读的 Mermaid graph TD 流程图。
func TestPipelineGraphTD(t *testing.T) {
	// 构建 3 级链
	s1 := NewStage("s1", StageConfig{Workers: 1, OutCap: 4}, nil, nil, passthrough[string])
	s2 := s1.NextStage("s2", StageConfig{Workers: 1, OutCap: 4}, nil, passthrough[string])
	s2.NextStage("s3", StageConfig{Workers: 1, OutCap: 4}, nil, passthrough[string])

	p := New[string, string](PipelineConfig{Name: "test"}).AddStage(s1)
	mermaid := p.GraphTD()

	// 验证结构：graph TD 头，边用 --> 连接
	if !strings.HasPrefix(mermaid, "graph TD\n") {
		t.Errorf("Mermaid 应以 graph TD 开头，got:\n%s", mermaid)
	}

	// 验证边：S1["s1"] --> S2["s2"] 形式
	for _, edge := range []string{`["s1"] -->`, `["s2"] -->`, `["s3"]`} {
		if !strings.Contains(mermaid, edge) {
			t.Errorf("Mermaid 缺少节点/边 %q，got:\n%s", edge, mermaid)
		}
	}
	// 验证 3 级链应有 2 条边
	if got := strings.Count(mermaid, "-->"); got != 2 {
		t.Errorf("边数量 = %d, want 2，got:\n%s", got, mermaid)
	}
}

// TestPipelineGraphTDNoStage 验证无 Stage 时 GraphTD 返回空图。
func TestPipelineGraphTDNoStage(t *testing.T) {
	p := New[string, string](PipelineConfig{})
	mermaid := p.GraphTD()
	if !strings.HasPrefix(mermaid, "graph TD\n") {
		t.Errorf("无 Stage 时应输出 graph TD 头，got:\n%s", mermaid)
	}
	if strings.Contains(mermaid, "-->") {
		t.Errorf("无 Stage 时不应有边，got:\n%s", mermaid)
	}
}

// passthrough 定义直通处理函数：in 原样返回。
func passthrough[T any](ctx context.Context, in T) (T, error) {
	return in, nil
}

// newPassThrough 创建直接透传输入的 Stage 测试辅助函数（带外部输入 Channel）。
func newPassThrough(name string) *Stage[string, string] {
	ch := make(chan string, 4)
	return NewStage(name, StageConfig{OutCap: 4}, ch, nil, func(ctx context.Context, in string) (string, error) {
		return in, nil
	})
}

// TestPipelineRunNoStage 验证未调用 AddStage 时 Run 返回 ErrPipelineEmpty。
func TestPipelineRunNoStage(t *testing.T) {
	p := New[string, string](PipelineConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := p.Run(ctx); err != ErrPipelineEmpty {
		t.Errorf("Run 无 Stage 错误 = %v, want %v", err, ErrPipelineEmpty)
	}
}

// TestStageCloseTimeout 验证 Close 超时后内部通过 cancel 已成功关闭（返回 nil）。
func TestStageCloseTimeout(t *testing.T) {
	in := make(chan string, 4)
	s := NewStage("s", StageConfig{Workers: 1, OutCap: 1}, in, nil, passthrough[string])
	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	// 不关闭 input，worker 阻塞等待 → Close 第一阶段超时后第二阶段取消 ctx 完成关闭。
	if err := s.Close(time.Millisecond); err != nil {
		t.Errorf("Close 应成功（通过 cancel 兜底）, got %v", err)
	}
	close(in)
}

// TestStageCloseBackpressure 验证 output 满且无人消费时 Close 能正常返回（不死锁）。
func TestStageCloseBackpressure(t *testing.T) {
	in := make(chan int, 10)
	// OutCap=1 且无下游消费者：output 很快满，worker 阻塞写。
	s := NewStage("s", StageConfig{Workers: 1, OutCap: 1}, in, nil, passthrough[int])
	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}
	// 快速填满 output 缓冲（OutCap=1），第二条数据让 worker 阻塞写。
	in <- 1
	in <- 2
	close(in)
	// Close 应能正常返回（内部有 timeout + cancel 兜底）。
	if err := s.Close(time.Second); err != nil {
		t.Errorf("背压场景 Close 应返回 nil, got %v", err)
	}
}

// TestMonitorConcurrent 验证 Monitor 并发注册与读取不产生竞态。
func TestMonitorConcurrent(t *testing.T) {
	mon := NewMonitor()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			sm := &StageMonitor{}
			sm.record(time.Millisecond, false)
			mon.Register("s", sm)
		}
		close(done)
	}()

	for i := 0; i < 100; i++ {
		mon.GenerateSummary()
	}
	<-done
}

// TestPipelineOutput 验证 Output 回调收集根 Stage 输出。
func TestPipelineOutput(t *testing.T) {
	collected := make(chan int, 8)
	root := NewStage("root", StageConfig{Workers: 2, OutCap: 8}, nil, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	pl := New[int, int](PipelineConfig{Name: "output"}).
		AddStage(root).
		Input(&MockSource[int]{Data: []int{1, 2, 3}}).
		Output(func(v int) {
			collected <- v
		})

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	_ = pl.Run(ctx)
	_ = pl.Close(time.Second)

	got := 0
	deadline := time.After(time.Second)
	for got < 3 {
		select {
		case <-collected:
			got++
		case <-deadline:
			t.Fatalf("Output 收到 %d 条, want 3", got)
		}
	}
}

// TestPipelineOutputChan 验证 OutputChan 返回根输出通道只读视图。
func TestPipelineOutputChan(t *testing.T) {
	root := NewStage("root", StageConfig{Workers: 2, OutCap: 8}, nil, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	pl := New[int, int](PipelineConfig{Name: "outchan"}).
		AddStage(root).
		Input(&MockSource[int]{Data: []int{7}})
	ch := pl.OutputChan()
	if ch == nil {
		t.Fatal("OutputChan 返回 nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_ = pl.Run(ctx)
	select {
	case v := <-ch:
		if v != 7 {
			t.Errorf("输出 = %d, want 7", v)
		}
	case <-time.After(time.Second):
		t.Fatal("OutputChan 超时")
	}
	_ = pl.Close(time.Second)
}
