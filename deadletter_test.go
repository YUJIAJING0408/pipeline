package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memSink 内存版 DeadLetterSink（自定义落盘示例：DB/MQ 可同样实现）。
type memSink struct {
	mu     sync.Mutex
	recs   []DeadLetterRecord
	closed atomic.Bool
}

func (m *memSink) Write(rec DeadLetterRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.recs = append(m.recs, rec)
	return nil
}

func (m *memSink) Close() error { m.closed.Store(true); return nil }

func (m *memSink) records() []DeadLetterRecord {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]DeadLetterRecord, len(m.recs))
	copy(out, m.recs)
	return out
}

// TestDeadLetterJSONL 验证默认 JSONL 落盘：Collect 模式失败数据写入 {stage}.dl.jsonl。
func TestDeadLetterJSONL(t *testing.T) {
	dir := t.TempDir()
	in := make(chan int, 3)
	out := make(chan int, 3)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		if x == 2 {
			return 0, errors.New("boom")
		}
		return x, nil
	}, 0, nil)
	wp.stageName = "dl-stage"
	wp.stageMonitor = &StageMonitor{}
	wp.errPol = &ErrPolicy{Mode: ErrModeCollect}
	wp.dlWriter = &deadLetterWriter{
		sink:    NewJSONLDeadLetterSink(dir),
		marshal: nil,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)
	in <- 1
	in <- 2 // 失败 → 进死信
	in <- 3
	close(in)
	wp.wait()

	// 死信文件应包含 1 条记录（值为 2 的输入）。
	raw, err := os.ReadFile(filepath.Join(dir, "dl-stage.dl.jsonl"))
	if err != nil {
		t.Fatalf("死信文件未创建: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 1 {
		t.Fatalf("死信行数 = %d, want 1\n%s", len(lines), raw)
	}
	var rec DeadLetterRecord
	if err := json.Unmarshal([]byte(lines[0]), &rec); err != nil {
		t.Fatalf("死信行不是合法 JSON: %v\n%s", err, lines[0])
	}
	if rec.Stage != "dl-stage" || rec.Code != CodeProcessing {
		t.Errorf("rec: stage=%s code=%s, want dl-stage/processing", rec.Stage, rec.Code)
	}
	if !strings.Contains(rec.ErrMsg, "boom") {
		t.Errorf("ErrMsg = %q, want 含 boom", rec.ErrMsg)
	}
	// Input 反序列化为 2。
	var input int
	if err := json.Unmarshal(rec.Input, &input); err != nil || input != 2 {
		t.Errorf("Input = %s -> %v, want 2", rec.Input, input)
	}
}

// TestDeadLetterCustomSink 验证用户自定义 sink（内存版）接收失败记录。
func TestDeadLetterCustomSink(t *testing.T) {
	sink := &memSink{}
	in := make(chan int, 2)
	out := make(chan int, 2)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		return 0, errors.New("always fail")
	}, 0, nil)
	wp.stageName = "custom"
	wp.errPol = &ErrPolicy{Mode: ErrModeCollect}
	wp.dlWriter = &deadLetterWriter{sink: sink, marshal: nil}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)
	in <- 42
	in <- 43
	close(in)
	wp.wait()

	recs := sink.records()
	if len(recs) != 2 {
		t.Fatalf("自定义 sink 收到 %d 条, want 2", len(recs))
	}
	if recs[0].Stage != "custom" || string(recs[0].Input) != "42" {
		t.Errorf("rec[0]: stage=%s input=%s", recs[0].Stage, recs[0].Input)
	}
}

// TestDeadLetterMarshalFallback 验证无法 JSON 序列化的输入降级为 fmt 文本。
func TestDeadLetterMarshalFallback(t *testing.T) {
	sink := &memSink{}
	in := make(chan chan int, 1) // chan 无法 json.Marshal
	out := make(chan chan int, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x chan int) (chan int, error) {
		return nil, errors.New("fail")
	}, 0, nil)
	wp.stageName = "fallback"
	wp.errPol = &ErrPolicy{Mode: ErrModeCollect}
	wp.dlWriter = &deadLetterWriter{sink: sink, marshal: nil}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)
	in <- make(chan int)
	close(in)
	wp.wait()

	recs := sink.records()
	if len(recs) != 1 {
		t.Fatalf("sink 收到 %d 条, want 1", len(recs))
	}
	if len(recs[0].Input) == 0 || strings.HasPrefix(string(recs[0].Input), "{") {
		t.Errorf("chan 输入应降级为 fmt 文本, got: %s", recs[0].Input)
	}
}

// TestDeadLetterCustomMarshal 验证用户自定义序列化回调生效。
func TestDeadLetterCustomMarshal(t *testing.T) {
	sink := &memSink{}
	in := make(chan string, 1)
	out := make(chan string, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x string) (string, error) {
		return "", errors.New("fail")
	}, 0, nil)
	wp.stageName = "marshal"
	wp.errPol = &ErrPolicy{Mode: ErrModeCollect}
	wp.dlWriter = &deadLetterWriter{
		sink: sink,
		marshal: func(v any) ([]byte, error) {
			return []byte("custom:" + v.(string)), nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)
	in <- "abc"
	close(in)
	wp.wait()

	recs := sink.records()
	if len(recs) != 1 || string(recs[0].Input) != "custom:abc" {
		t.Errorf("自定义 marshal 未生效, got: %s", recs[0].Input)
	}
}

// TestDeadLetterRetryExhausted 验证 RetryFallback 重试耗尽且无降级时进死信，Retried 记录重试次数。
func TestDeadLetterRetryExhausted(t *testing.T) {
	sink := &memSink{}
	in := make(chan int, 1)
	out := make(chan int, 1)
	wp := newWorkerPool(1, in, out, func(ctx context.Context, x int) (int, error) {
		return 0, context.DeadlineExceeded
	}, 0, nil)
	wp.stageName = "retry-dl"
	wp.errPol = &ErrPolicy{
		Mode:       ErrModeRetryFallback,
		MaxRetry:   2,
		RetryDelay: time.Millisecond,
	}
	wp.dlWriter = &deadLetterWriter{sink: sink, marshal: nil}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wp.start(ctx)
	in <- 9
	close(in)
	wp.wait()

	recs := sink.records()
	if len(recs) != 1 {
		t.Fatalf("sink 收到 %d 条, want 1", len(recs))
	}
	if recs[0].Code != CodeTimeout {
		t.Errorf("Code = %s, want timeout", recs[0].Code)
	}
	if recs[0].Retried != 2 {
		t.Errorf("Retried = %d, want 2", recs[0].Retried)
	}
}

// TestDeadLetterReaderAndReplay 验证 JSONLDeadLetterReader 读取 + DeadLetterReplaySource 重放。
func TestDeadLetterReaderAndReplay(t *testing.T) {
	dir := t.TempDir()
	sink := NewJSONLDeadLetterSink(dir)
	_ = sink.Write(DeadLetterRecord{TS: time.Now(), Stage: "s", Input: json.RawMessage("100"), ErrMsg: "e1"})
	_ = sink.Write(DeadLetterRecord{TS: time.Now(), Stage: "s", Input: json.RawMessage("200"), ErrMsg: "e2"})
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}

	// 读取。
	reader := NewJSONLDeadLetterReader(dir)
	recs, err := reader.Read("s")
	if err != nil || len(recs) != 2 {
		t.Fatalf("读取死信失败: %v, len=%d", err, len(recs))
	}

	// 重放：把死信输入作为新 Pipeline 的输入源。
	var seen []int
	replay := &DeadLetterReplaySource[int]{
		Reader: reader,
		Stage:  "s",
		Unmarshal: func(raw json.RawMessage) (int, error) {
			var v int
			err := json.Unmarshal(raw, &v)
			return v, err
		},
	}
	out := make(chan int, 2)
	ctx := context.Background()
	if err := replay.Start(ctx, out); err != nil {
		t.Fatal(err)
	}
	seen = append(seen, <-out, <-out)
	if len(seen) != 2 || seen[0] != 100 || seen[1] != 200 {
		t.Errorf("重放结果 = %v, want [100 200]", seen)
	}
}

// TestPipelineDeadLetterEndToEnd 验证 Pipeline 层端到端：FailFast 失败数据进死信 + DrainDeadLetters。
func TestPipelineDeadLetterEndToEnd(t *testing.T) {
	dir := t.TempDir()
	s1 := NewStage("numbers", StageConfig{Workers: 1, OutCap: 8}, nil, nil,
		func(ctx context.Context, x int) (int, error) {
			if x < 0 {
				return 0, WithCode(errors.New("negative invalid"), CodeInvalidInput)
			}
			return x, nil
		})
	s1.NextStage("echo", StageConfig{Workers: 1, OutCap: 8}, nil, func(ctx context.Context, x int) (int, error) {
		return x, nil
	})

	pl := New[int, int](PipelineConfig{
		Name:       "dl-e2e",
		LogDir:     dir,
		DeadLetter: &DeadLetterConfig{Dir: dir},
	}).AddStage(s1).Input(&MockSource[int]{Data: []int{1, -5, 3}})

	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); cancel() }()
	_ = pl.Run(ctx)
	if err := pl.Close(2 * time.Second); err != nil {
		t.Fatal(err)
	}

	// 死信文件应包含 x=-5（InvalidInput 分类）。
	recs, err := pl.DrainDeadLetters("numbers")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("DrainDeadLetters = %d 条, want 1", len(recs))
	}
	if recs[0].Code != CodeInvalidInput {
		t.Errorf("Code = %s, want invalid_input（WithCode 显式标注应保留）", recs[0].Code)
	}
	var input int
	if err := json.Unmarshal(recs[0].Input, &input); err != nil || input != -5 {
		t.Errorf("Input = %v, want -5", recs[0].Input)
	}
}