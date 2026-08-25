package pipeline

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// LogRotation 描述 StageLogger 文件的轮转策略（D-38）。
// 三个维度独立可配可关（均为 0 时关闭轮转，保持旧行为）：
//   - MaxSizeMB：单文件大小上限（MiB），达到即轮转；
//   - MaxBackups：保留的轮转文件数量（{stage}.log.1 ~ .N），超过删除最旧；0 = 保留全部；
//   - MaxAgeDays：轮转文件最长保留天数（按修改时间清理），0 = 不按天数清理。
type LogRotation struct {
	MaxSizeMB  int
	MaxBackups int
	MaxAgeDays int
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

	maxSizeBytes int64         // 轮转大小阈值（字节），0 关闭
	maxBackups   int           // 保留轮转文件数，0 保留全部
	maxAge       time.Duration // 轮转文件保留时长，0 不按天数清理
	size         int64         // 当前文件已写字节数

	mu sync.Mutex // 串行化写文件 + Sync + 轮转
}

// NewStageLogger 在 dir 下创建 {dir}/{stageName}.log 并返回对应 Logger（不启用轮转）。
// dir 不存在时自动创建（MkdirAll）。
func NewStageLogger(stageName, dir string, level LogLevel) (*StageLogger, error) {
	return NewStageLoggerWithRotation(stageName, dir, level, LogRotation{})
}

// NewStageLoggerWithRotation 在 dir 下创建 {dir}/{stageName}.log，并按 rotation 配置轮转（D-38）。
// dir 不存在时自动创建（MkdirAll）；rotation 全零等价于 NewStageLogger。
func NewStageLoggerWithRotation(stageName, dir string, level LogLevel, rotation LogRotation) (*StageLogger, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, stageName+".log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	info, _ := f.Stat()
	var cur int64
	if info != nil {
		cur = info.Size()
	}
	return &StageLogger{
		file:         f,
		stage:        stageName,
		level:        level,
		path:         path,
		maxSizeBytes: int64(rotation.MaxSizeMB) * 1024 * 1024,
		maxBackups:   rotation.MaxBackups,
		maxAge:       time.Duration(rotation.MaxAgeDays) * 24 * time.Hour,
		size:         cur,
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

// write 序列化一行 JSON 并追加写入文件（并发安全，级别过滤；达到大小阈值时先轮转再写入）。
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
	if l.maxSizeBytes > 0 && l.size+int64(len(data)) > l.maxSizeBytes {
		l.rotateLocked()
	}
	_, _ = l.file.Write(append(data, '\n'))
	l.size += int64(len(data))
}

// rotateLocked 执行一次轮转：当前文件变 .log.1，旧备份依次后移，超过 MaxBackups 删除最旧，
// 并按 MaxAgeDays 清理过期文件（调用方需持有 mu）。
func (l *StageLogger) rotateLocked() {
	_ = l.file.Sync()
	_ = l.file.Close()

	if l.maxBackups > 0 {
		// 删除超过保留上限的最旧备份（先删，避免后移时它成为 .N+1 游离文件）。
		_ = os.Remove(l.seqPath(l.maxBackups + 1))
		for i := l.maxBackups; i >= 1; i-- {
			if _, err := os.Stat(l.seqPath(i)); err == nil {
				_ = os.Rename(l.seqPath(i), l.seqPath(i+1))
			}
		}
	} else {
		// 不限份数：后移所有已存在的备份。
		for i := l.maxSeq(); i >= 1; i-- {
			if _, err := os.Stat(l.seqPath(i)); err == nil {
				_ = os.Rename(l.seqPath(i), l.seqPath(i+1))
			}
		}
	}
	_ = os.Rename(l.path, l.seqPath(1))

	l.cleanupByAge()

	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		l.file = nil // 打开失败时标记不可写，后续写调用由调用方自行感知
		return
	}
	l.file = f
	l.size = 0
}

// seqPath 返回第 n 个备份文件路径（{path}.n）。
func (l *StageLogger) seqPath(n int) string {
	return fmt.Sprintf("%s.%d", l.path, n)
}

// maxSeq 扫描目录返回当前最大的备份序号（MaxBackups=0 不限份数时用于后移）。
func (l *StageLogger) maxSeq() int {
	entries, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		return 0
	}
	prefix := l.stage + ".log."
	max := 0
	for _, e := range entries {
		name := e.Name()
		if name == l.stage+".log" || !strings.HasPrefix(name, prefix) {
			continue
		}
		if n, err := strconv.Atoi(strings.TrimPrefix(name, prefix)); err == nil && n > max {
			max = n
		}
	}
	return max
}

// cleanupByAge 删除修改时间超过 maxAge 的轮转备份文件（best-effort）。
func (l *StageLogger) cleanupByAge() {
	if l.maxAge <= 0 {
		return
	}
	entries, err := os.ReadDir(filepath.Dir(l.path))
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-l.maxAge)
	prefix := l.stage + ".log."
	for _, e := range entries {
		name := e.Name()
		if name == l.stage+".log" || !strings.HasPrefix(name, prefix) {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			_ = os.Remove(filepath.Join(filepath.Dir(l.path), name))
		}
	}
}

// Path 返回日志文件的完整路径。
func (l *StageLogger) Path() string { return l.path }

// Sync 刷盘日志文件。
func (l *StageLogger) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return os.ErrClosed
	}
	return l.file.Sync()
}

// Close 刷盘并关闭日志文件。
func (l *StageLogger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	if err := l.file.Sync(); err != nil {
		return err
	}
	return l.file.Close()
}
