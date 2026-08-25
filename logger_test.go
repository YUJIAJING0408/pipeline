package pipeline

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// bigFields 构造一条足够大的日志行（padSize 控制字段大小），用于快速触发轮转。
func bigFields(padSize int) []LogField {
	return []LogField{F("pad", strings.Repeat("x", padSize))}
}

// listBackups 返回 {stage}.log.N 后缀文件的文件名切片（不含主文件）。
func listBackups(t *testing.T, dir, stage string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取目录失败: %v", err)
	}
	prefix := stage + ".log."
	var backups []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), prefix) {
			backups = append(backups, e.Name())
		}
	}
	return backups
}

// TestStageLoggerRotationBySize 验证单文件达到 MaxSizeMB 后触发轮转（产生 .log.1）。
func TestStageLoggerRotationBySize(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLoggerWithRotation("stage-r", dir, LogLevelInfo, LogRotation{MaxSizeMB: 1})
	if err != nil {
		t.Fatalf("NewStageLoggerWithRotation 失败: %v", err)
	}
	defer l.Close()

	// 每条约 128KB（字段 + JSON 开销），写 12 条 ≈1.5MB 确保超过 1MB 阈值并轮转。
	for i := 0; i < 12; i++ {
		l.Infow("big", bigFields(128*1024)...)
	}
	if err := l.Sync(); err != nil {
		t.Fatalf("Sync 失败: %v", err)
	}

	backups := listBackups(t, dir, "stage-r")
	if len(backups) == 0 {
		t.Fatal("达到大小阈值后应产生轮转备份文件，got 0")
	}
	if _, err := os.Stat(filepath.Join(dir, "stage-r.log")); err != nil {
		t.Errorf("主文件应存在: %v", err)
	}
}

// TestStageLoggerRotationShift 验证多次轮转后缀依次推移（.log.1 → .log.2，新备份成为 .log.1）。
func TestStageLoggerRotationShift(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLoggerWithRotation("stage-s", dir, LogLevelInfo, LogRotation{MaxSizeMB: 1})
	if err != nil {
		t.Fatalf("NewStageLoggerWithRotation 失败: %v", err)
	}
	defer l.Close()

	// 写 3 批（每批超过 1MB），应产生 .log.1 与 .log.2。
	for b := 0; b < 3; b++ {
		for i := 0; i < 5; i++ {
			l.Infow("big", bigFields(128*1024)...)
		}
		_ = l.Sync()
	}

	backups := listBackups(t, dir, "stage-s")
	if len(backups) < 2 {
		t.Fatalf("应产生至少 2 个备份，got %v", backups)
	}

	// 最新备份的内容应与最后一次写入一致；.log.2 存在说明 .log.1 被推移。
	if size := fileSize(t, filepath.Join(dir, "stage-s.log.1")); size == 0 {
		t.Error(".log.1 不应为空（当前轮转所得）")
	}
	if size := fileSize(t, filepath.Join(dir, "stage-s.log.2")); size == 0 {
		t.Error(".log.2 不应为空（旧 .log.1 被推移）")
	}
}

// TestStageLoggerRotationMaxBackups 验证超过 MaxBackups 后最旧备份被删除。
func TestStageLoggerRotationMaxBackups(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLoggerWithRotation("stage-m", dir, LogLevelInfo, LogRotation{MaxSizeMB: 1, MaxBackups: 2})
	if err != nil {
		t.Fatalf("NewStageLoggerWithRotation 失败: %v", err)
	}
	defer l.Close()

	// 写 4 批（每批超过 1MB），超过保留上限 2 个。
	for b := 0; b < 4; b++ {
		for i := 0; i < 5; i++ {
			l.Infow("big", bigFields(128*1024)...)
		}
		_ = l.Sync()
	}

	backups := listBackups(t, dir, "stage-m")
	if len(backups) > 2 {
		t.Fatalf("超过 MaxBackups=2 应删除最旧备份，got %v", backups)
	}
}

// TestStageLoggerRotationMaxAge 验证超过 MaxAgeDays 的备份被清理。
func TestStageLoggerRotationMaxAge(t *testing.T) {
	dir := t.TempDir()
	// 先手动造一个"过期"备份：立即创建 .log.1 并回拨 mtime 到 10 天前。
	old := filepath.Join(dir, "stage-a.log.1")
	if err := os.WriteFile(old, []byte("stale"), 0o644); err != nil {
		t.Fatalf("创建过期备份失败: %v", err)
	}
	past := time.Now().Add(-10 * 24 * time.Hour)
	if err := os.Chtimes(old, past, past); err != nil {
		t.Fatalf("回拨 mtime 失败: %v", err)
	}

	// MaxAgeDays=3：轮转时旧备份超过 3 天应被删除。
	l, err := NewStageLoggerWithRotation("stage-a", dir, LogLevelInfo, LogRotation{MaxSizeMB: 1, MaxAgeDays: 3})
	if err != nil {
		t.Fatalf("NewStageLoggerWithRotation 失败: %v", err)
	}
	defer l.Close()

	for i := 0; i < 12; i++ {
		l.Infow("big", bigFields(128*1024)...)
	}
	_ = l.Sync()

	// 过期备份经轮转推移为 .log.2 后被按天清理；.log.1 此时为新轮转内容（不检查）。
	if _, err := os.Stat(filepath.Join(dir, "stage-a.log.2")); err == nil {
		t.Error("超过 MaxAgeDays 的过期备份（推移到 .log.2）应被清理")
	}
}

// TestStageLoggerNoRotation 验证旧构造函数 NewStageLogger 不启用轮转（兼容旧行为）。
func TestStageLoggerNoRotation(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLogger("stage-n", dir, LogLevelInfo)
	if err != nil {
		t.Fatalf("NewStageLogger 失败: %v", err)
	}
	defer l.Close()

	for i := 0; i < 5; i++ {
		l.Infow("big", bigFields(128*1024)...)
	}
	_ = l.Sync()

	if backups := listBackups(t, dir, "stage-n"); len(backups) != 0 {
		t.Errorf("旧构造函数不应产生轮转备份，got %v", backups)
	}
	// 主文件仍完整可读。
	if size := fileSize(t, filepath.Join(dir, "stage-n.log")); size == 0 {
		t.Error("主文件不应为空")
	}
}

// TestStageLoggerRotationConcurrent 验证并发写入 + 轮转下无 race（由 -race 检测），且行数不丢。
func TestStageLoggerRotationConcurrent(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLoggerWithRotation("stage-c", dir, LogLevelInfo, LogRotation{MaxSizeMB: 1, MaxBackups: 2})
	if err != nil {
		t.Fatalf("NewStageLoggerWithRotation 失败: %v", err)
	}
	defer l.Close()

	const goroutines = 8
	const per = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				l.Infow("c", bigFields(2*1024)...)
			}
		}()
	}
	wg.Wait()
	_ = l.Sync()

	// 所有行必须落盘（主文件 + 备份行数合计 = goroutines × per + 1 空行不计）。
	total := 0
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取目录失败: %v", err)
	}
	for _, e := range entries {
		if e.Name() == "stage-c.log" || strings.HasPrefix(e.Name(), "stage-c.log.") {
			data := mustReadLog(t, filepath.Join(dir, e.Name()))
			for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
				if line != "" {
					total++
				}
			}
		}
	}
	if total != goroutines*per {
		t.Fatalf("轮转后行数 = %d, want %d（不应丢行）", total, goroutines*per)
	}
}

// fileSize 返回文件字节数（不存在返回 0）。
func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}

// countLogLines 统计目录下所有 stage 相关日志文件的总行数（主文件 + 轮转备份）。
func countLogLines(t *testing.T, dir, stage string) int {
	t.Helper()
	total := 0
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("读取目录失败: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if name == stage+".log" || strings.HasPrefix(name, stage+".log.") {
			data := mustReadLog(t, filepath.Join(dir, name))
			for _, line := range strings.Split(strings.TrimSpace(data), "\n") {
				if line != "" {
					total++
				}
			}
		}
	}
	return total
}

// TestStageLoggerSampleInfo 验证 sampleRate=N 时 info 只记录第 N、2N、3N…条。
func TestStageLoggerSampleInfo(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLoggerWithConfig("stage-sp", dir, LogConfig{
		Level:      LogLevelInfo,
		SampleRate: 3, // 每 3 条记 1 条
	})
	if err != nil {
		t.Fatalf("NewStageLoggerWithConfig 失败: %v", err)
	}
	defer l.Close()

	const writes = 10
	for i := 0; i < writes; i++ {
		l.Infow("sample", F("i", i))
	}
	_ = l.Sync()

	// 第 3、6、9 条被记录（sampleRate=3），其余丢弃：writes/3 = 3 行。
	want := writes / 3
	if got := countLogLines(t, dir, "stage-sp"); got != want {
		t.Fatalf("info 采样后行数 = %d, want %d", got, want)
	}
}

// TestStageLoggerSampleErrorAlways 验证 sampleRate 下 error 恒记（不采样）。
func TestStageLoggerSampleErrorAlways(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLoggerWithConfig("stage-se", dir, LogConfig{
		Level:      LogLevelError,
		SampleRate: 3,
	})
	if err != nil {
		t.Fatalf("NewStageLoggerWithConfig 失败: %v", err)
	}
	defer l.Close()

	const writes = 7
	for i := 0; i < writes; i++ {
		l.Errorf("boom %d", i)
	}
	_ = l.Sync()

	if got := countLogLines(t, dir, "stage-se"); got != writes {
		t.Fatalf("error 恒记：行数 = %d, want %d", got, writes)
	}
}

// TestStageLoggerSampleWarnAlways 验证 sampleRate 下 warn 恒记（不采样）。
func TestStageLoggerSampleWarnAlways(t *testing.T) {
	dir := t.TempDir()
	l, err := NewStageLoggerWithConfig("stage-sw", dir, LogConfig{
		Level:      LogLevelWarn,
		SampleRate: 5,
	})
	if err != nil {
		t.Fatalf("NewStageLoggerWithConfig 失败: %v", err)
	}
	defer l.Close()

	const writes = 6
	for i := 0; i < writes; i++ {
		l.Warnf("careful %d", i)
	}
	_ = l.Sync()

	if got := countLogLines(t, dir, "stage-sw"); got != writes {
		t.Fatalf("warn 恒记：行数 = %d, want %d", got, writes)
	}
}

// TestStageLoggerSampleDisabled 验证 sampleRate 0 / 1 时全量记录（不采样）。
func TestStageLoggerSampleDisabled(t *testing.T) {
	for _, rate := range []int{0, 1} {
		dir := t.TempDir()
		l, err := NewStageLoggerWithConfig("stage-sd", dir, LogConfig{
			Level:      LogLevelInfo,
			SampleRate: rate,
		})
		if err != nil {
			t.Fatalf("NewStageLoggerWithConfig(rate=%d) 失败: %v", rate, err)
		}
		for i := 0; i < 5; i++ {
			l.Infow("full", F("i", i))
		}
		_ = l.Sync()
		_ = l.Close()

		if got := countLogLines(t, dir, "stage-sd"); got != 5 {
			t.Fatalf("sampleRate=%d 应全量记录：行数 = %d, want 5", rate, got)
		}
	}
}

// TestStageLoggerSamplingPipeline 验证 PipelineConfig.LogSampleRate 经 params 生效于 Stage 日志。
func TestStageLoggerSamplingPipeline(t *testing.T) {
	dir := t.TempDir()
	in := make(chan string, 32)
	s := NewStage("stage-pl", StageConfig{Workers: 1, OutCap: 4}, in, nil, func(ctx context.Context, x string) (string, error) {
		return x, nil
	})

	params := map[string]any{
		"logDir":        dir,
		"logEnabled":    true,
		"logSampleRate": 2,
	}
	if err := s.Start(context.Background(), params); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	for i := 0; i < 5; i++ {
		in <- "a"
	}
	close(in)
	_ = s.Close(time.Second)

	// Stage 启动/关闭各写 1 条 info（处理过程不写日志）；sampleRate=2 → 第 1 条丢弃、第 2 条保留 = 1 行。
	if got := countLogLines(t, dir, "stage-pl"); got != 1 {
		t.Fatalf("Pipeline 采样后行数 = %d, want 1（2 条 info 每 2 记 1）", got)
	}
}
