package pipeline

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrMode 定义 Pipeline 的错误处理模式。
type ErrMode uint8

const (
	// ErrModeFailFast 任一 Stage 失败 → 整条 Pipeline 立即停止。
	ErrModeFailFast ErrMode = iota
	// ErrModeCollect 记录错误，继续处理，结束时在汇总报告中呈现。
	ErrModeCollect
	// ErrModeRetryFallback 按 MaxRetry/RetryDelay 重试，仍失败则走 Fallback 降级。
	ErrModeRetryFallback
)

// ErrPolicy 统一描述错误处理策略。
//
// 设计取舍：ErrPolicy 刻意保持非泛型，以便作为 Pipeline 级共享配置；
// Fallback / OnError 回调中的 in/out 使用 any，由框架在调用点做安全类型断言。
type ErrPolicy struct {
	Mode       ErrMode
	MaxRetry   int                                                    // RetryFallback 模式：最大重试次数
	RetryDelay time.Duration                                          // RetryFallback 模式：基础重试间隔（第 1 次）
	RetryBackoff float64                                              // 指数退避乘数（D-31）：第 n 次间隔 = RetryDelay × Backoff^(n-1)；≤1 时为固定间隔
	Fallback   func(ctx context.Context, in any) (out any, err error) // 重试耗尽后的降级函数
	OnError    func(err error, stageName string, in any)              // 错误回调（所有模式触发）
}

var (
	// ErrPipelineStarted 表示 Pipeline 已启动，Run 不可重复调用。
	ErrPipelineStarted = errors.New("pipeline: already started")
	// ErrPipelineEmpty 表示 Pipeline 未注册任何 Stage，Run 无法启动。
	ErrPipelineEmpty = errors.New("pipeline: no stages registered")
	// ErrStageMissingInput 表示 Stage 的输入 Channel 为 nil，无法启动。
	ErrStageMissingInput = errors.New("pipeline: stage input channel is nil")
	// ErrStageMissingOutput 表示 Stage 的输出 Channel 为 nil，无法启动。
	ErrStageMissingOutput = errors.New("pipeline: stage output channel is nil")
	// ErrStageAlreadyRunning 表示 Stage 已启动，Start 不可重复调用。
	ErrStageAlreadyRunning = errors.New("pipeline: stage already running")
	// ErrCloseTimeout 表示 Stage.Close 排空在途数据超时。
	ErrCloseTimeout = errors.New("pipeline: stage close timeout")
)

// ErrorCode 表示单条数据处理的错误分类，供框架做针对性决策
// （日志分类、指标统计、重试策略、死信路由）。
type ErrorCode uint8

const (
	// CodeUnknown 未分类错误（兜底）。
	CodeUnknown ErrorCode = iota
	// CodeTimeout 单条处理超时（如 StageConfig.Timeout 触发、下游调用超时）。
	CodeTimeout
	// CodeInvalidInput 输入数据非法（格式/校验失败），重试无意义。
	CodeInvalidInput
	// CodeProcessing 业务处理失败（process 函数返回的普通错误）。
	CodeProcessing
	// CodeSystem 基础设施故障（DB / 网络 / 外部服务不可用）。
	CodeSystem
)

// String 返回错误码的可读名称（与日志输出一致）。
func (c ErrorCode) String() string {
	switch c {
	case CodeTimeout:
		return "timeout"
	case CodeInvalidInput:
		return "invalid_input"
	case CodeProcessing:
		return "processing"
	case CodeSystem:
		return "system"
	default:
		return "unknown"
	}
}

// 与 ErrorCode 一一对应的哨兵错误，供 errors.Is 判型：
// errors.Is(err, pipeline.ErrTimeout) 等价于 err 的 Code == CodeTimeout。
var (
	ErrTimeout       = errors.New("pipeline: processing timeout")
	ErrInvalidInput  = errors.New("pipeline: invalid input")
	ErrProcessing    = errors.New("pipeline: processing failed")
	ErrSystemFailure = errors.New("pipeline: system failure")
)

// StageError 是带处理上下文的错误包装（workerPool 自动对 process 返回的错误包装）。
//
// 区别于裸 error：携带 Stage 名、输入值、耗时与错误分类；
// 通过 Unwrap/Is 支持 errors.Is / errors.As 穿透与哨兵匹配。
type StageError struct {
	// Stage 出错的 Stage 名称。
	Stage string
	// Code 错误分类（由 wrapStageError 自动推断，或用户 WithCode 显式指定）。
	Code ErrorCode
	// Input 触发错误的输入值（可能携带敏感数据，谨慎落盘）。
	Input any
	// Latency 单条处理耗时（包装时填充）。
	Latency time.Duration
	// Err 底层原始错误。
	Err error
}

// Error 实现 error 接口，格式便于日志阅读。
func (e *StageError) Error() string {
	return fmt.Sprintf("pipeline: [stage=%s code=%s] %v", e.Stage, e.Code, e.Err)
}

// Unwrap 透传底层错误，使 errors.Is(err, underlyingErr) 可穿透。
func (e *StageError) Unwrap() error { return e.Err }

// Is 支持哨兵错误匹配：errors.Is(err, pipeline.ErrTimeout) 命中对应 Code。
func (e *StageError) Is(target error) bool {
	switch {
	case errors.Is(e.Err, target):
		return true
	case target == ErrTimeout && e.Code == CodeTimeout:
		return true
	case target == ErrInvalidInput && e.Code == CodeInvalidInput:
		return true
	case target == ErrProcessing && e.Code == CodeProcessing:
		return true
	case target == ErrSystemFailure && e.Code == CodeSystem:
		return true
	}
	return false
}

// WithCode 将 err 包装为带有显式分类的 StageError（供 process 内部标注错误类型）。
// 若 err 为 nil 返回 nil；若已是 StageError 则更新其 Code。
func WithCode(err error, code ErrorCode) error {
	if err == nil {
		return nil
	}
	var se *StageError
	if errors.As(err, &se) {
		se.Code = code
		return se
	}
	return &StageError{Code: code, Err: err}
}

// AsStageError 从错误链中提取 *StageError，未找到返回 nil。
func AsStageError(err error) *StageError {
	var se *StageError
	if errors.As(err, &se) {
		return se
	}
	return nil
}

// wrapStageError 将 process 返回的错误统一包装为 *StageError：
//   - 已是 StageError：保留已有 Code，补全 Stage/Input/Latency；
//   - 裸错误：依据错误内容推断分类（context.DeadlineExceeded → CodeTimeout，其余 → CodeProcessing）。
func wrapStageError(stage string, input any, err error, latency time.Duration) *StageError {
	var se *StageError
	if errors.As(err, &se) {
		se.Stage = stage
		se.Input = input
		se.Latency = latency
		return se
	}
	code := classifyError(err)
	return &StageError{
		Stage:   stage,
		Code:    code,
		Input:   input,
		Latency: latency,
		Err:     err,
	}
}

// classifyError 依据错误内容推断分类（无显式标注时的默认值）。
func classifyError(err error) ErrorCode {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return CodeTimeout
	}
	return CodeProcessing
}
