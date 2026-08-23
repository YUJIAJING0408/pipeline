package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// DeadLetterRecord 是单条死信记录：处理失败且最终未进入下游的数据快照。
//
// Input 以 json.RawMessage 保存原始输入的序列化结果（TS 前后端一致）；
// 反序列化回具体类型由消费方（DeadLetterReplaySource / 用户）负责。
type DeadLetterRecord struct {
	TS      time.Time       `json:"ts"`
	Stage   string          `json:"stage"`
	Code    ErrorCode       `json:"code"`
	Input   json.RawMessage `json:"input"`
	ErrMsg  string          `json:"err"`
	Latency time.Duration   `json:"latencyNs"`
	Retried int             `json:"retried"`
}

// DeadLetterSink 是死信记录的落盘抽象，框架不关心落到哪里：
// 文件（默认 JSONL）/ 数据库 / 消息队列均可实现本接口接入。
//
// 契约：Write 可能被多个 worker 并发调用，实现必须线程安全；
// Write 失败返回 error，由框架记录日志后继续主流程（不阻塞、不重试）。
type DeadLetterSink interface {
	Write(rec DeadLetterRecord) error
	Close() error
}

// DeadLetterConfig 是死信队列的运行时配置，nil 表示不启用。
type DeadLetterConfig struct {
	// Sink 为落盘实现；nil 时使用默认 JSONLDeadLetterSink（dir 目录）。
	Sink DeadLetterSink
	// Dir 仅在 Sink 为 nil 时生效：JSONL 文件目录（默认 LogDir）。
	Dir string
	// MarshalInput 自定义输入序列化（默认 json.Marshal）。
	// 返回的字节会原样写入记录（不一定是合法 JSON，由消费方自理）。
	MarshalInput func(v any) ([]byte, error)
}

// JSONLDeadLetterSink 是默认死信落盘实现：按 Stage 名分文件
// {dir}/{stageName}.dl.jsonl，每行一条 JSON 死信记录。
type JSONLDeadLetterSink struct {
	mu      sync.Mutex
	files   map[string]*os.File // stageName → 文件句柄（懒创建）
	dir     string
	closed  bool
}

// NewJSONLDeadLetterSink 创建 JSONL 死信落盘（dir 不存在时自动创建）。
func NewJSONLDeadLetterSink(dir string) *JSONLDeadLetterSink {
	return &JSONLDeadLetterSink{files: make(map[string]*os.File), dir: dir}
}

// defaultDeadLetterDir 返回死信 JSONL 文件目录：
// 优先用 params 中的 logDir，其次默认 ./logs。
func defaultDeadLetterDir(params map[string]any) string {
	if dir, ok := params["logDir"].(string); ok && dir != "" {
		return dir
	}
	return "./logs"
}

// Write 将一条死信记录追加写入 {dir}/{stage}.dl.jsonl。
func (s *JSONLDeadLetterSink) Write(rec DeadLetterRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("pipeline: dead letter sink closed")
	}

	f, err := s.fileFor(rec.Stage)
	if err != nil {
		return err
	}
	data, err := json.Marshal(&rec)
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	if err != nil {
		return err
	}
	return f.Sync()
}

// fileFor 懒创建某 Stage 的死信文件（线程安全，调用方需持锁）。
func (s *JSONLDeadLetterSink) fileFor(stage string) (*os.File, error) {
	if f, ok := s.files[stage]; ok {
		return f, nil
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(s.dir, stage+".dl.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.files[stage] = f
	return f, nil
}

// Close 关闭所有已打开的死信文件。
func (s *JSONLDeadLetterSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	for name, f := range s.files {
		if err := f.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close %s: %w", name, err)
		}
	}
	s.files = nil
	return firstErr
}

// DeadLetterReader 从落盘介质读回死信记录（Drain / Replay 用）。
type DeadLetterReader interface {
	Read(stage string) ([]DeadLetterRecord, error)
}

// JSONLDeadLetterReader 读取 {dir}/{stage}.dl.jsonl 中全部死信记录。
type JSONLDeadLetterReader struct {
	dir string
}

// NewJSONLDeadLetterReader 创建 JSONL 读取器。
func NewJSONLDeadLetterReader(dir string) *JSONLDeadLetterReader {
	return &JSONLDeadLetterReader{dir: dir}
}

// Read 读取某 Stage 的全部死信记录；文件不存在返回空切片（非错误）。
func (r *JSONLDeadLetterReader) Read(stage string) ([]DeadLetterRecord, error) {
	f, err := os.Open(filepath.Join(r.dir, stage+".dl.jsonl"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []DeadLetterRecord
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec DeadLetterRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			continue // 跳过损坏行，不阻断读取
		}
		out = append(out, rec)
	}
	return out, scanner.Err()
}

// DeadLetterReplaySource 将某 Stage 的死信记录作为输入源重新注入 Pipeline。
//
// 用户在 Unmarshal 回调中把 Input（json.RawMessage）断言回 T1 类型；
// 内部复用现有 InputSource 机制，天然继承背压与错误策略。
type DeadLetterReplaySource[T1 any] struct {
	// Reader 死信读取器（默认 JSONLDeadLetterReader）。
	Reader DeadLetterReader
	// Stage 要重放的 Stage 名。
	Stage string
	// Unmarshal 将死信记录的 Input 反序列化为 T1；nil 时先尝试 json.Unmarshal，
	// 失败则跳过该条。
	Unmarshal func(raw json.RawMessage) (T1, error)
}

// Start 实现 InputSource：顺序发送死信记录中的输入，ctx 取消时提前返回。
func (s *DeadLetterReplaySource[T1]) Start(ctx context.Context, out chan<- T1) error {
	if s.Reader == nil {
		return fmt.Errorf("pipeline: DeadLetterReplaySource.Reader 未设置")
	}
	recs, err := s.Reader.Read(s.Stage)
	if err != nil {
		return err
	}
	unmarshal := s.Unmarshal
	if unmarshal == nil {
		unmarshal = func(raw json.RawMessage) (T1, error) {
			var v T1
			err := json.Unmarshal(raw, &v)
			return v, err
		}
	}
	for _, rec := range recs {
		v, err := unmarshal(rec.Input)
		if err != nil {
			continue // 无法还原类型的数据跳过
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- v:
		}
	}
	return nil
}