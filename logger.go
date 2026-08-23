package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogLevel 表示结构化日志的级别（数值越大越严重）。
type LogLevel int

const (
	// LogLevelDebug 调试级：细粒度诊断信息，默认关闭。
	LogLevelDebug LogLevel = iota
	// LogLevelInfo 信息级：常规运行事件（默认级别）。
	LogLevelInfo
	// LogLevelWarn 警告级：可恢复的异常情况。
	LogLevelWarn
	// LogLevelError 错误级：处理失败 / 需要人工介入。
	LogLevelError
)

// String 返回级别名（小写，与 JSON 输出一致）。
func (l LogLevel) String() string {
	switch l {
	case LogLevelDebug:
		return "debug"
	case LogLevelInfo:
		return "info"
	case LogLevelWarn:
		return "warn"
	case LogLevelError:
		return "error"
	default:
		return "info"
	}
}

// ParseLogLevel 将字符串解析为 LogLevel；未知值返回 LogLevelInfo。
// 支持大小写不敏感："debug"/"info"/"warn"/"error"。
func ParseLogLevel(s string) LogLevel {
	switch s {
	case "debug":
		return LogLevelDebug
	case "warn", "warning":
		return LogLevelWarn
	case "error":
		return LogLevelError
	case "info":
		fallthrough
	default:
		return LogLevelInfo
	}
}

// LogField 是结构化日志的附加字段（key-value 对）。
type LogField struct {
	Key   string
	Value any
}

// F 构造一个 LogField（便于调用方书写 l.Infow("msg", F("k", v))）。
func F(key string, value any) LogField { return LogField{Key: key, Value: value} }

// logLine 是写入文件的一行 JSON 结构（字段顺序固定，便于 grep/jq 处理）。
type logLine struct {
	TS     string         `json:"ts"`
	Stage  string         `json:"stage"`
	Level  string         `json:"level"`
	Msg    string         `json:"msg"`
	Fields map[string]any `json:"fields,omitempty"`
}

// StageLogger 管理单个 Stage 的独立日志文件（D-08），输出 JSON 结构化日志。
//
// 约定：日志目录下每个 Stage 一个文件，命名 {stageName}.log；
// 每行一条 JSON，包含 ts/stage/level/msg 及可选 fields。
// level 下方的记录被过滤（默认 Info 级）。
type StageLogger struct {
	file  *os.File
	stage string
	level LogLevel
	path  string // 日志文件完整路径，便于外部定位

	mu sync.Mutex // 串行化写文件 + Sync
}

// NewStageLogger 在 dir 下创建 {dir}/{stageName}.log 并返回对应 Logger。
// dir 不存在时自动创建（MkdirAll）。
func NewStageLogger(stageName, dir string, level LogLevel) (*StageLogger, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, stageName+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &StageLogger{
		file:  f,
		stage: stageName,
		level: level,
		path:  path,
	}, nil
}

// SetLevel 运行期调整日志级别（如动态开启 Debug 排障）。
func (l *StageLogger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// Level 返回当前日志级别。
func (l *StageLogger) Level() LogLevel {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.level
}

// Enabled 返回 level 是否 ≥ 当前配置级别（写日志前预判，避免无谓格式化）。
func (l *StageLogger) Enabled(level LogLevel) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return level >= l.level
}

// Debugf / Infof / Warnf / Errorf 按级别写一条格式化消息（无附加字段）。
func (l *StageLogger) Debugf(format string, args ...any) {
	l.write(LogLevelDebug, fmt.Sprintf(format, args...), nil)
}

func (l *StageLogger) Infof(format string, args ...any) {
	l.write(LogLevelInfo, fmt.Sprintf(format, args...), nil)
}

func (l *StageLogger) Warnf(format string, args ...any) {
	l.write(LogLevelWarn, fmt.Sprintf(format, args...), nil)
}

func (l *StageLogger) Errorf(format string, args ...any) {
	l.write(LogLevelError, fmt.Sprintf(format, args...), nil)
}

// Debugw / Infow / Warnw / Errorw 按级别写一条消息并附加结构化字段。
func (l *StageLogger) Debugw(msg string, fields ...LogField) {
	l.write(LogLevelDebug, msg, fieldMap(fields))
}

func (l *StageLogger) Infow(msg string, fields ...LogField) {
	l.write(LogLevelInfo, msg, fieldMap(fields))
}

func (l *StageLogger) Warnw(msg string, fields ...LogField) {
	l.write(LogLevelWarn, msg, fieldMap(fields))
}

func (l *StageLogger) Errorw(msg string, fields ...LogField) {
	l.write(LogLevelError, msg, fieldMap(fields))
}

// Printf 兼容旧接口：以 Info 级别写一条消息。
func (l *StageLogger) Printf(format string, args ...any) {
	l.write(LogLevelInfo, fmt.Sprintf(format, args...), nil)
}

// fieldMap 将 LogField 变参折叠为 map（空输入返回 nil，omitempty 省略输出）。
func fieldMap(fields []LogField) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]any, len(fields))
	for _, f := range fields {
		m[f.Key] = f.Value
	}
	return m
}

// write 序列化一行 JSON 并追加写入文件（并发安全，级别过滤）。
func (l *StageLogger) write(level LogLevel, msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if level < l.level {
		return
	}
	line := logLine{
		TS:     time.Now().Format(time.RFC3339Nano),
		Stage:  l.stage,
		Level:  level.String(),
		Msg:    msg,
		Fields: fields,
	}
	data, err := json.Marshal(&line)
	if err != nil {
		// 字段含无法序列化值（如 chan/func）：降级为仅 msg 的行。
		data = []byte(fmt.Sprintf(`{"ts":%q,"stage":%q,"level":%q,"msg":%q}`,
			line.TS, line.Stage, line.Level, line.Msg))
	}
	_, _ = l.file.Write(append(data, '\n'))
}

// Path 返回日志文件的完整路径。
func (l *StageLogger) Path() string { return l.path }

// Sync 刷盘日志文件。
func (l *StageLogger) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.file.Sync()
}

// Close 刷盘并关闭日志文件。
func (l *StageLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.file.Sync(); err != nil {
		return err
	}
	return l.file.Close()
}