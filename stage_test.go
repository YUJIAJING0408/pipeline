package pipeline

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

// TestNewStage 验证 NewStage 构造与 Name/Config/Channel 注入（D-14）。
func TestNewStage(t *testing.T) {
	cfg := StageConfig{Workers: 2, OutCap: 16, Timeout: 0}
	inputCh := make(chan string, 8)
	s := NewStage("stage-a", cfg, inputCh, nil, func(ctx context.Context, in string) (string, error) {
		return in, nil
	})

	if s.Name() != "stage-a" {
		t.Errorf("Name() = %q, want stage-a", s.Name())
	}
	if got := s.Config(); !reflect.DeepEqual(got, cfg) {
		t.Errorf("Config() = %+v, want %+v", got, cfg)
	}
	if s.input != inputCh {
		t.Error("input 应等于传入的 inputChan")
	}
	if cap(s.output) != 16 {
		t.Errorf("output 容量 = %d, want 16", cap(s.output))
	}
}

// TestNewStageNegativeOutCap 验证负 OutCap 自动归一为 0。
func TestNewStageNegativeOutCap(t *testing.T) {
	inputCh := make(chan string, 4)
	s := NewStage("stage-a", StageConfig{OutCap: -5}, inputCh, nil, func(ctx context.Context, in string) (string, error) {
		return in, nil
	})
	if cap(s.output) != 0 {
		t.Errorf("负 OutCap 应归一为 0, got output=%d", cap(s.output))
	}
}

// TestStageInOutChan 验证 InChan/OutChan 返回私有 Channel 的方向化视图（D-13/D-14）。
func TestStageInOutChan(t *testing.T) {
	inputCh := make(chan int, 1)
	s := NewStage("stage-a", StageConfig{OutCap: 1}, inputCh, nil, func(ctx context.Context, in int) (int, error) {
		return in, nil
	})

	inputCh <- 7
	if got := <-s.InChan(); got != 7 {
		t.Errorf("InChan 读取 = %d, want 7", got)
	}

	s.OutChan() <- 9
	if got := <-s.output; got != 9 {
		t.Errorf("OutChan 写入后读取 = %d, want 9", got)
	}
}

// TestStageProcess 验证 Process 转发到注入的处理函数。
func TestStageProcess(t *testing.T) {
	s := NewStage("stage-a", StageConfig{}, make(chan int), nil, func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})

	out, err := s.Process(context.Background(), 21)
	if err != nil {
		t.Fatalf("Process 失败: %v", err)
	}
	if out != 42 {
		t.Errorf("Process(21) = %d, want 42", out)
	}
}

// TestStageProcessError 验证处理函数返回的错误原样透出。
func TestStageProcessError(t *testing.T) {
	want := errors.New("boom")
	s := NewStage("stage-a", StageConfig{}, make(chan int), nil, func(ctx context.Context, in int) (int, error) {
		return 0, want
	})

	if _, err := s.Process(context.Background(), 1); !errors.Is(err, want) {
		t.Errorf("Process 错误 = %v, want %v", err, want)
	}
}

// TestStageNextStage 验证 NextStage 创建子 Stage，并为子 Stage 分配独立输入 Channel（D-21）。
func TestStageNextStage(t *testing.T) {
	s1 := NewStage("s1", StageConfig{Workers: 1, OutCap: 4}, make(chan int, 4), nil, func(ctx context.Context, in int) (int, error) {
		return in * 2, nil
	})
	s2 := s1.NextStage("s2", StageConfig{Workers: 1, OutCap: 4}, nil, func(ctx context.Context, in int) (int, error) {
		return in + 1, nil
	})

	// D-21：子 Stage 获得独立输入 Channel（非共享父 output），父将其记录在 subOutChans。
	if s2.input == s1.output {
		t.Error("s2.input 应为独立 Channel，不应复用 s1.output")
	}
	if s2.input != s1.subOutChans[0] {
		t.Error("s2.input 应等于 s1.subOutChans[0]（同一 Channel 实例）")
	}
	if len(s1.subStages) != 1 {
		t.Errorf("s1.subStages 数量 = %d, want 1", len(s1.subStages))
	}
	if len(s1.subOutChans) != 1 {
		t.Errorf("s1.subOutChans 数量 = %d, want 1", len(s1.subOutChans))
	}
	if s2.Name() != "s2" {
		t.Errorf("s2.Name() = %q, want s2", s2.Name())
	}
}

// TestStageFanoutBroadcast 验证广播语义（D-21）：父 2 条产出被复制分发给 2 个独立分支，
// 每个分支都收到全部 2 条数据（非竞争消费）。
func TestStageFanoutBroadcast(t *testing.T) {
	inCh := make(chan int, 4)
	s1 := NewStage("s1", StageConfig{Workers: 1, OutCap: 4}, inCh, nil, func(ctx context.Context, in int) (int, error) {
		return in * 10, nil // 产出 10、20
	})
	branchA := s1.NextStage("A", StageConfig{Workers: 1, OutCap: 4}, nil, func(ctx context.Context, in int) (int, error) {
		return in, nil
	})
	branchB := s1.NextStage("B", StageConfig{Workers: 1, OutCap: 4}, nil, func(ctx context.Context, in int) (int, error) {
		return in, nil
	})

	// 独立通道：A/B 输入不同（各自 subOutChan）。
	if branchA.input == branchB.input {
		t.Error("分支 A/B 应使用独立输入 Channel")
	}

	if err := s1.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	inCh <- 1
	inCh <- 2
	close(inCh)

	// 等待广播转发 + 分支处理完成。
	time.Sleep(50 * time.Millisecond)
	_ = s1.Close(time.Second)

	// 分支 A 与 B 都收到全部 2 条产出（10、20）。
	gotA := map[int]bool{}
	for range 2 {
		gotA[<-branchA.output] = true
	}
	gotB := map[int]bool{}
	for range 2 {
		gotB[<-branchB.output] = true
	}
	for _, want := range []int{10, 20} {
		if !gotA[want] {
			t.Errorf("分支 A 缺少 %d, gotA=%v", want, gotA)
		}
		if !gotB[want] {
			t.Errorf("分支 B 缺少 %d, gotB=%v", want, gotB)
		}
	}
}

// TestStageStartMissingInput 验证 input 为 nil 时 Start 拒绝启动。
func TestStageStartMissingInput(t *testing.T) {
	s := &Stage[string, string]{name: "s", config: StageConfig{Workers: 1}, output: make(chan string, 4)}
	if err := s.Start(context.Background(), nil); !errors.Is(err, ErrStageMissingInput) {
		t.Errorf("Start 错误 = %v, want %v", err, ErrStageMissingInput)
	}
}

// TestStageStartMissingOutput 验证 output 为 nil 时 Start 拒绝启动。
func TestStageStartMissingOutput(t *testing.T) {
	s := &Stage[string, string]{name: "s", config: StageConfig{Workers: 1}, input: make(chan string, 4)}
	if err := s.Start(context.Background(), nil); !errors.Is(err, ErrStageMissingOutput) {
		t.Errorf("Start 错误 = %v, want %v", err, ErrStageMissingOutput)
	}
}

// TestStageStartTwice 验证重复 Start 返回 ErrStageAlreadyRunning。
func TestStageStartTwice(t *testing.T) {
	in := make(chan string, 4)
	s := NewStage("s", StageConfig{Workers: 1, OutCap: 4}, in, nil, func(ctx context.Context, x string) (string, error) {
		return x, nil
	})

	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatalf("首次 Start 失败: %v", err)
	}
	if err := s.Start(context.Background(), nil); !errors.Is(err, ErrStageAlreadyRunning) {
		t.Errorf("二次 Start 错误 = %v, want %v", err, ErrStageAlreadyRunning)
	}

	close(in)
}

// TestStageStartFlow 验证 Start 后数据端到端流转：input → Process → output。
func TestStageStartFlow(t *testing.T) {
	in := make(chan string, 4)
	s := NewStage("s", StageConfig{Workers: 2, OutCap: 4}, in, nil, func(ctx context.Context, x string) (string, error) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
			return x + "!", nil
		}
	})

	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	in <- "hello"
	in <- "world"
	close(in)
	s.pool.wait()

	got := map[string]bool{}
	for range 2 {
		got[<-s.output] = true
	}
	if !got["hello!"] || !got["world!"] {
		t.Errorf("输出应为 {hello!, world!}, got %v", got)
	}
}

// TestStageStartRecursive 验证 Start 递归启动子 Stage（subStages 先于自身启动）。
func TestStageStartRecursive(t *testing.T) {
	in := make(chan int, 4)
	s1 := NewStage("s1", StageConfig{Workers: 1, OutCap: 4}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	s2 := s1.NextStage("s2", StageConfig{Workers: 1, OutCap: 4}, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})

	if err := s1.Start(context.Background(), nil); err != nil {
		t.Fatalf("s1 Start 失败: %v", err)
	}
	if !s2.running.Load() {
		t.Error("subStage s2 未在 s1.Start 时启动")
	}
	if !s1.running.Load() {
		t.Error("s1 自身未启动")
	}

	close(in)
	_ = s1.Close(time.Second)
}

// TestStageCloseGraceful 验证 Close：排空 → 关闭 output → 幂等。
func TestStageCloseGraceful(t *testing.T) {
	in := make(chan string, 4)
	s := NewStage("s", StageConfig{Workers: 2, OutCap: 4}, in, nil, func(ctx context.Context, x string) (string, error) {
		return x + "!", nil
	})

	if err := s.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	in <- "hello"
	in <- "world"
	close(in)

	if err := s.Close(time.Second); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if err := s.Close(time.Second); err != nil {
		t.Errorf("重复 Close 应返回 nil, got %v", err)
	}

	got := map[string]bool{}
	for range 2 {
		got[<-s.output] = true
	}
	if !got["hello!"] || !got["world!"] {
		t.Errorf("输出应为 {hello!, world!}, got %v", got)
	}
	if _, ok := <-s.output; ok {
		t.Error("output 应已被 Close 关闭")
	}
}

// TestStageCloseRecursive 验证 Close 递归关闭子 Stage：自身先排空关闭，再依次关闭子 Stage。
func TestStageCloseRecursive(t *testing.T) {
	in := make(chan int, 4)
	s1 := NewStage("s1", StageConfig{Workers: 1, OutCap: 4}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	s2 := s1.NextStage("s2", StageConfig{Workers: 1, OutCap: 4}, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})

	if err := s1.Start(context.Background(), nil); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	in <- 1
	in <- 2
	close(in)

	if err := s1.Close(time.Second); err != nil {
		t.Fatalf("Close 失败: %v", err)
	}
	if s2.running.Load() {
		t.Error("subStage s2 应在 s1.Close 后也关闭")
	}
}

// TestStageCloseIdempotentBeforeStart 验证未 Start 时 Close 直接返回 nil。
func TestStageCloseIdempotentBeforeStart(t *testing.T) {
	s := NewStage("s", StageConfig{Workers: 1, OutCap: 2}, make(chan int, 2), nil, func(ctx context.Context, in int) (int, error) {
		return in, nil
	})
	if err := s.Close(time.Second); err != nil {
		t.Errorf("未启动的 Close 应返回 nil, got %v", err)
	}
}

// TestStageRouteFilter 验证路由条件（D-25）：只满足 routeFn 的数据进入该分支。
func TestStageRouteFilter(t *testing.T) {
	in := make(chan int, 10)
	s := NewStage("root", StageConfig{Workers: 2, OutCap: 10}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	// 分支 A：只接收偶数
	childA := s.NextStage("even", StageConfig{Workers: 1, OutCap: 10},
		func(x int) bool { return x%2 == 0 },
		func(ctx context.Context, x int) (int, error) { return x, nil })
	// 分支 B：nil routeFn = 放行所有
	childB := s.NextStage("all", StageConfig{Workers: 1, OutCap: 10}, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5; i++ {
		in <- i
	}
	close(in)
	s.pool.wait()

	// 验证分支 A 只收到偶数（2, 4）
	gotA := map[int]bool{}
	for range 2 {
		v := <-childA.output
		gotA[v] = true
	}
	if !gotA[2] || !gotA[4] {
		t.Errorf("even 分支: got %v, want {2,4}", gotA)
	}
	if gotA[1] || gotA[3] || gotA[5] {
		t.Errorf("even 分支不应收到奇数: %v", gotA)
	}
	// 验证分支 B 收到全部（1, 2, 3, 4, 5）
	gotB := map[int]bool{}
	for range 5 {
		v := <-childB.output
		gotB[v] = true
	}
	for i := 1; i <= 5; i++ {
		if !gotB[i] {
			t.Errorf("all 分支缺少 %d, got %v", i, gotB)
		}
	}
}

// TestStageRouteDefaultBranch 验证默认路由分支（D-32）：routeFn 全不匹配时投递默认分支。
func TestStageRouteDefaultBranch(t *testing.T) {
	in := make(chan int, 10)
	s := NewStage("root", StageConfig{Workers: 2, OutCap: 10}, in, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})
	// 分支 A：只接收负数。
	childA := s.NextStage("neg", StageConfig{Workers: 1, OutCap: 10},
		func(x int) bool { return x < 0 },
		func(ctx context.Context, x int) (int, error) { return x, nil })
	// 分支 B：默认兜底（nil routeFn）。
	childB := s.NextStage("default", StageConfig{Workers: 1, OutCap: 10}, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	s.SetDefaultBranch(childB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for _, x := range []int{-1, 1, -2, 2, 3} {
		in <- x
	}
	close(in)
	s.pool.wait()

	// 分支 A 只收到负数（-1, -2）。
	gotA := map[int]bool{}
	for range 2 {
		v := <-childA.output
		gotA[v] = true
	}
	if !gotA[-1] || !gotA[-2] {
		t.Errorf("neg 分支: got %v, want {-1,-2}", gotA)
	}
	// 分支 B（默认）收到正数（1, 2, 3）——负数被 A 接收，正数兜底到 B。
	gotB := map[int]bool{}
	for range 3 {
		v := <-childB.output
		gotB[v] = true
	}
	if !gotB[1] || !gotB[2] || !gotB[3] {
		t.Errorf("default 分支: got %v, want {1,2,3}", gotB)
	}
	if gotB[-1] || gotB[-2] {
		t.Errorf("default 分支不应收到负数（已由 neg 分支接收）: %v", gotB)
	}
}
