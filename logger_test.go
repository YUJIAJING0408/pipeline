package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// mustReadLog 读取日志文件全部内容（失败时 t.Fatal）。
func mustReadLog(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("读取日志失败: %v", err)
	}
	return string(data)
}

// TestStageLoggerCreatesFile 验证日志文件创建在正确路径。
func TestStageLoggerCreatesFile(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLogger("stage-x", dir, LogLevelInfo)
	if err != nil {
		t.Fatalf("NewStageLogger 失败: %v", err)
	}
	defer l.Close()

	if _, err := os.Stat(filepath.Join(dir, "stage-x.log")); err != nil {
		t.Errorf("日志文件未创建: %v", err)
	}
	if l.Path() != filepath.Join(dir, "stage-x.log") {
		t.Errorf("Path() = %q, want %q", l.Path(), filepath.Join(dir, "stage-x.log"))
	}
}

// TestStageLoggerPrintf 验证 Printf 以 Info 级别写入 JSON 行且内容正确。
func TestStageLoggerPrintf(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLogger("stage-w", dir, LogLevelInfo)
	if err != nil {
		t.Fatalf("NewStageLogger 失败: %v", err)
	}
	defer l.Close()

	l.Printf("hello %d", 42)
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}

	data := mustReadLog(t, filepath.Join(dir, "stage-w.log"))

	// 每行一条 JSON，且字段齐全
	line := data[strings.Index(data, "{") : strings.LastIndex(data, "}")+1]
	var rec struct {
		TS    string `json:"ts"`
		Stage string `json:"stage"`
		Level string `json:"level"`
		Msg   string `json:"msg"`
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("日志行不是合法 JSON: %v\ngot: %s", err, data)
	}
	if rec.Stage != "stage-w" {
		t.Errorf("stage = %q, want stage-w", rec.Stage)
	}
	if rec.Level != "info" {
		t.Errorf("level = %q, want info", rec.Level)
	}
	if rec.Msg != "hello 42" {
		t.Errorf("msg = %q, want hello 42", rec.Msg)
	}
	if !strings.HasSuffix(data, "\n") {
		t.Error("JSON 行应以换行结尾")
	}
}

// TestStageLoggerJsonFields 验证 Infow 写入 fields 并序列化为合法 JSON。
func TestStageLoggerJsonFields(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLogger("stage-f", dir, LogLevelInfo)
	if err != nil {
		t.Fatalf("NewStageLogger 失败: %v", err)
	}
	defer l.Close()

	l.Infow("item processed", F("id", 7), F("ok", true))
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}

	data := mustReadLog(t, filepath.Join(dir, "stage-f.log"))
	line := data[strings.Index(data, "{") : strings.LastIndex(data, "}")+1]
	var rec struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal([]byte(line), &rec); err != nil {
		t.Fatalf("日志行不是合法 JSON: %v\n%s", err, data)
	}
	if _, ok := rec.Fields["id"]; !ok {
		t.Errorf("fields 缺少 id，got: %v", rec.Fields)
	}
	if got := rec.Fields["id"]; got != float64(7) {
		t.Errorf("id = %v, want 7", got)
	}
	if okv, _ := rec.Fields["ok"].(bool); !okv {
		t.Errorf("ok = %v, want true", rec.Fields["ok"])
	}
}

// TestStageLoggerLevelFilter 验证级别过滤：低于配置级别的记录不落盘。
func TestStageLoggerLevelFilter(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLogger("stage-level", dir, LogLevelWarn)
	if err != nil {
		t.Fatalf("NewStageLogger 失败: %v", err)
	}
	defer l.Close()

	l.Debugf("should not appear")
	l.Infof("should not appear")
	l.Warnf("warn appears")
	l.Errorf("error appears")
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}

	data := mustReadLog(t, filepath.Join(dir, "stage-level.log"))
	if strings.Contains(data, "should not appear") {
		t.Errorf("低于配置级别(Warn)的记录不应落盘，got: %s", data)
	}
	if !strings.Contains(data, "warn appears") || !strings.Contains(data, "error appears") {
		t.Errorf("Warn/Error 级记录应落盘，got: %s", data)
	}
}

// TestStageLoggerSetLevel 验证运行期 SetLevel 动态调整过滤。
func TestStageLoggerSetLevel(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLogger("stage-dyn", dir, LogLevelInfo)
	if err != nil {
		t.Fatalf("NewStageLogger 失败: %v", err)
	}
	defer l.Close()

	l.Debugf("debug before")
	l.SetLevel(LogLevelDebug)
	l.Debugf("debug after")
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}

	data := mustReadLog(t, filepath.Join(dir, "stage-dyn.log"))
	if !strings.Contains(data, "debug after") {
		t.Errorf("SetLevel(Debug) 后 Debug 记录应落盘，got: %s", data)
	}
	if strings.Contains(data, "debug before") {
		t.Errorf("SetLevel 前的 Debug 记录不应落盘，got: %s", data)
	}
}

// TestParseLogLevel 验证字符串解析。
func TestParseLogLevel(t *testing.T) {
	cases := map[string]LogLevel{
		"debug": LogLevelDebug,
		"INFO":  LogLevelInfo,
		"warn":  LogLevelWarn,
		"error": LogLevelError,
		"bogus": LogLevelInfo,
	}
	for in, want := range cases {
		if got := ParseLogLevel(in); got != want {
			t.Errorf("ParseLogLevel(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestStageLoggerMkdirAll 验证目录不存在时自动创建。
func TestStageLoggerMkdirAll(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "logs", "stages")
	l, err := NewStageLogger("stage-d", dir, LogLevelInfo)
	if err != nil {
		t.Fatalf("NewStageLogger 失败: %v", err)
	}
	defer l.Close()

	if fi, err := os.Stat(dir); err != nil || !fi.IsDir() {
		t.Errorf("日志目录未自动创建: %v", err)
	}
}

// TestStageStartCreatesLogger 验证 Start 时从 params 读取 logDir/logEnabled 创建日志文件。
func TestStageStartCreatesLogger(t *testing.T) {
	dir := t.TempDir()
	in := make(chan string, 4)
	s := NewStage("s", StageConfig{Workers: 1, OutCap: 4}, in, nil, func(ctx context.Context, x string) (string, error) {
		return x, nil
	})

	params := map[string]any{
		"logDir":     dir,
		"logEnabled": true,
	}
	if err := s.Start(context.Background(), params); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	if s.logger == nil {
		t.Fatal("logger 未创建（应为非 nil）")
	}
	if _, err := os.Stat(filepath.Join(dir, "s.log")); err != nil {
		t.Errorf("日志文件未创建: %v", err)
	}

	close(in)
	_ = s.Close(time.Second)
}

// TestStageStartLogDisabled 验证 LogEnabled=false 时不创建日志。
func TestStageStartLogDisabled(t *testing.T) {
	dir := t.TempDir()
	in := make(chan string, 4)
	s := NewStage("s", StageConfig{Workers: 1, OutCap: 4}, in, nil, func(ctx context.Context, x string) (string, error) {
		return x, nil
	})

	params := map[string]any{
		"logDir":     dir,
		"logEnabled": false,
	}
	if err := s.Start(context.Background(), params); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	if s.logger != nil {
		t.Fatal("LogEnabled=false 时 logger 应为 nil")
	}

	close(in)
	_ = s.Close(time.Second)
}

// TestStageCloseLog 验证 Close 时写入 JSON 关闭事件到日志文件。
func TestStageCloseLog(t *testing.T) {
	dir := t.TempDir()
	in := make(chan string, 4)
	s := NewStage("s", StageConfig{Workers: 1, OutCap: 4}, in, nil, func(ctx context.Context, x string) (string, error) {
		return x, nil
	})

	params := map[string]any{
		"logDir":     dir,
		"logEnabled": true,
	}
	if err := s.Start(context.Background(), params); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	in <- "a"
	close(in)
	_ = s.Close(time.Second)

	data := mustReadLog(t, filepath.Join(dir, "s.log"))
	if !strings.Contains(data, "stage closed") {
		t.Errorf("日志应包含 close 事件，got: %s", data)
	}
	if !strings.Contains(data, `"stage":"s"`) {
		t.Errorf("close 事件应为 JSON 且含 stage 字段，got: %s", data)
	}
}
