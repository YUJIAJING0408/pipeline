package pipeline

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// PipelineConfig 描述 Pipeline 的整体配置（D-18）。
type PipelineConfig struct {
	// Name 为 Pipeline 名称，用于日志/监控标识。
	Name string
	// InputBufferSize 为首节点 inchan 缓冲区大小（0 表示无缓冲，天然背压）。
	InputBufferSize int
	// LogDir 为日志存储位置，空则默认 ./logs。
	LogDir string
	// LogEnabled 为整体日志开关；false 时各 Stage 日志不落盘。
	LogEnabled bool
	// LogLevel 为各 Stage 日志级别（默认 LogLevelInfo，Debug 级默认关闭）。
	LogLevel LogLevel
	// LogRotation 为各 Stage 日志轮转策略（D-38）；全零 = 不轮转。
	LogRotation LogRotation
	// LogSampleRate 为各 Stage 日志采样分母（D-39）：info/debug 每第 N 条记 1 条，
	// error/warn 恒记；0 或 1 = 全量。
	LogSampleRate int
	// DeadLetter 为死信队列配置；nil 表示不启用（D-23）。
	// Sink 非 nil 时使用用户提供实现（DB/MQ 等）；否则用默认 JSONL 落盘到 Dir。
	DeadLetter *DeadLetterConfig
}

// Pipeline 编排多个 Stage 串联执行（D-01 / D-03）。
//
// T1 = 输入源数据类型，T2 = 根 Stage 输入类型。
// Stage 之间的连接通过 NextStage 完成（非 Pipeline 管理）。
type Pipeline[T1, T2 any] struct {
	// config 为整体配置（D-18）。
	config PipelineConfig
	// stage 为根 Stage（AddStage 设置）。
	stage *Stage[T1, T2]
	// sources 为输入源集合（当前支持多源同时注入）。
	sources []InputSource[T1]
	// errPol 为全局错误策略，注入每个 Stage。
	errPol ErrPolicy
	// monitor 为全局链路时间监控。
	monitor *Monitor
	// output 首节点输入通道（New 创建，缓冲 = InputBufferSize）。
	output chan T1

	// started 标记 Run 是否已被调用（防止重复启动）。
	started atomic.Bool
	// closed 标记 Close 是否已完成。
	closed atomic.Bool
}

// New 创建带配置的空 Pipeline（D-18），随后通过 Builder 方法组装。
// LogDir 为空时默认 ./logs。
func New[T1, T2 any](cfg PipelineConfig) *Pipeline[T1, T2] {
	if cfg.LogDir == "" {
		cfg.LogDir = "./logs"
	}
	if cfg.InputBufferSize < 0 {
		cfg.InputBufferSize = 0
	}
	return &Pipeline[T1, T2]{
		config:  cfg,
		monitor: NewMonitor(),
		output:  make(chan T1, cfg.InputBufferSize),
	}
}

// AddStage 设置根 Stage（链式连接通过 NextStage 完成，非 Pipeline 管理）。
//
// 将 Pipeline 的 output 通道（New 时创建，缓冲 = InputBufferSize）设为根 Stage 的 input。
func (p *Pipeline[T1, T2]) AddStage(stage *Stage[T1, T2]) *Pipeline[T1, T2] {
	p.stage = stage
	// 绑定Pipeline的输出和第一个Stage的输入
	stage.input = p.output
	return p
}

// Input 注册输入源；框架内置 MockSource，用户也可实现 InputSource[T]。
// 可多次调用以注册多个输入源（多源同时注入到首节点）。
func (p *Pipeline[T1, T2]) Input(src InputSource[T1]) *Pipeline[T1, T2] {
	p.sources = append(p.sources, src)
	return p
}

// ErrorPolicy 设置全局错误策略（D-04），会注入每个 Stage。
func (p *Pipeline[T1, T2]) ErrorPolicy(pol ErrPolicy) *Pipeline[T1, T2] {
	p.errPol = pol
	return p
}

// LogDir 设置日志输出目录（默认 ./logs，每个 Stage 一个文件）。
// 便捷覆盖 PipelineConfig.LogDir。
func (p *Pipeline[T1, T2]) LogDir(dir string) *Pipeline[T1, T2] {
	p.config.LogDir = dir
	return p
}

// MetricsMonitor 返回 Pipeline 内部的 Monitor，供 MetricsServer / 自定义巡检接入。
func (p *Pipeline[T1, T2]) MetricsMonitor() *Monitor {
	return p.monitor
}

// Output 收集根 Stage 的全部输出，回调处理最终结果（D-34）。
//
// 语义：启动一个 goroutine 消费根 Stage 的 output 通道，逐条调用 fn；
// Pipeline 关闭（Close）后通道自动关闭，消费 goroutine 随退出。
// 适合"链最终输出需要一个消费者"的场景：省去手工 OutChan + for-range goroutine。
//
// 注意：fn 内应尽快返回，避免成为处理瓶颈（消费 goroutine 单线程）。
func (p *Pipeline[T1, T2]) Output(fn func(v T2)) *Pipeline[T1, T2] {
	if p.stage == nil || fn == nil {
		return p
	}
	go func() {
		for v := range p.stage.output {
			fn(v)
		}
	}()
	return p
}

// OutputChan 返回根 Stage 输出通道的只读视图，调用方自行消费。
// 与 Output 的区别：不启动消费 goroutine，由调用方控制消费节奏（如批量聚合）。
func (p *Pipeline[T1, T2]) OutputChan() <-chan T2 {
	if p.stage == nil {
		return nil
	}
	return p.stage.output
}

// DrainDeadLetters 读取指定 Stage 的全部死信记录（读后不清除文件）。
//
// 适用默认 JSONL 落盘：目录取 DeadLetterConfig.Dir（未设置时用 LogDir）。
// 自定义 sink 若实现了 DeadLetterReader 也支持；否则返回错误提示自行读取。
func (p *Pipeline[T1, T2]) DrainDeadLetters(stage string) ([]DeadLetterRecord, error) {
	if p.config.DeadLetter == nil {
		return nil, errors.New("pipeline: dead letter 未启用（PipelineConfig.DeadLetter 为 nil）")
	}
	if p.config.DeadLetter.Sink != nil {
		if r, ok := p.config.DeadLetter.Sink.(DeadLetterReader); ok {
			return r.Read(stage)
		}
		return nil, fmt.Errorf("pipeline: 自定义 sink %T 未实现 DeadLetterReader，请自行读取", p.config.DeadLetter.Sink)
	}
	dir := p.config.DeadLetter.Dir
	if dir == "" {
		dir = p.config.LogDir
	}
	return NewJSONLDeadLetterReader(dir).Read(stage)
}

// Describe 返回链路上各 Stage 名称的摘要（示例与调试用）。
func (p *Pipeline[T1, T2]) Describe() {
	if p.stage != nil {
		p.stage.Describe(p.config.Name)
	}
}

// GraphTD 返回 Mermaid graph TD 格式的流程图，可直接在支持 Mermaid 的
// 编辑器 / Markdown / CI 管道中渲染。
func (p *Pipeline[T1, T2]) GraphTD() string {
	var b strings.Builder
	b.WriteString("graph TD\n")
	if p.stage != nil {
		w := &graphTDWriter{b: &b, ids: make(map[Stager]string)}
		p.stage.GraphTD(w)
	}
	return b.String()
}

// Run 启动 Pipeline（D-17）。
//
// 流程：
//  1. 校验未启动（重复调用返回 ErrPipelineStarted）且至少有一个 Stage
//     （否则返回 ErrPipelineEmpty）；
//  2. 从前往后逐个调 Stage.Start，构建 Context 链：s1.Start(ctx) →
//     s2.Start(s1.ForwardCtx()) → …；
//  3. 并发启动所有输入源向首节点通道（p.output，New 时创建、AddStage 接管为根 Stage input）
//     写入，全部返回后统一 close；
//  4. 阻塞等待 ctx.Done()（持续运行语义）。
//
// 返回后由调用方触发 Pipeline.Close 完成级联优雅关闭。
func (p *Pipeline[T1, T2]) Run(ctx context.Context) error {
	if p.started.Load() {
		return ErrPipelineStarted
	}
	if p.stage == nil {
		return ErrPipelineEmpty
	}
	p.started.Store(true)

	params := map[string]any{
		"logDir":        p.config.LogDir,
		"logEnabled":    p.config.LogEnabled,
		"logLevel":      p.config.LogLevel,
		"logRotation":   p.config.LogRotation,
		"logSampleRate": p.config.LogSampleRate,
		"monitor":       p.monitor,
		"errPol":        &p.errPol,
		"deadLetter":    p.config.DeadLetter,
	}
	err := p.stage.Start(ctx, params)
	if err != nil {
		return err
	}

	// 2. 输入源注入：全部结束统一 close 首节点入口，触发自然排空。
	if p.output != nil {
		p.startSources(ctx, p.output)
	}

	// 3. 阻塞直至外部取消（持续运行）。
	<-ctx.Done()
	return nil
}

// startSources 并发启动所有输入源写入 ch；全部返回后由 goroutine 统一 close。
func (p *Pipeline[T1, T2]) startSources(ctx context.Context, ch chan T1) {
	var wg sync.WaitGroup
	for _, src := range p.sources {
		wg.Add(1)
		go func(s InputSource[T1]) {
			defer wg.Done()
			_ = s.Start(ctx, ch)
		}(src)
	}
	go func() {
		wg.Wait()
		close(ch)
	}()
}

// Close 触发级联优雅关闭（D-17，详见 ARCHITECTURE.md §6）。
//
// 过程：从前往后依序调用各 Stage.Close(timeout)，每个 Stage 完成自身排空、
// 关闭 output、取消 forwardCtx 后通知下一个，直至最后一个 Stage 关闭。
// 返回首个错误。未启动/已关闭时幂等返回 nil。
func (p *Pipeline[T1, T2]) Close(timeout time.Duration) error {
	if !p.started.Load() {
		return nil
	}
	if p.closed.Load() {
		return nil
	}
	p.closed.Store(true)

	// 无输入源时，关闭首节点输入通道触发自然排空（避免依赖 timeout 兜底）。
	if len(p.sources) == 0 && p.output != nil {
		close(p.output)
	}

	var firstErr error
	if err := p.stage.Close(timeout); err != nil {
		firstErr = err
	}
	return firstErr
}
