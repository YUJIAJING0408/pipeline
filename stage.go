package pipeline

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Stager interface {
	Start(ctx context.Context, params map[string]any) error
	Close(drainTimeout time.Duration) error
	Name() string
	Describe(parent string)
	GraphTD(w *graphTDWriter)
}

// graphTDWriter 是 Mermaid graph TD 流程图的递归生成器。
// 维护节点 ID 分配，输出 graph TD 语法的节点与边。
type graphTDWriter struct {
	b   *strings.Builder
	ids map[Stager]string
	seq int
}

// nodeID 为 Stager 分配或复用节点 ID（S1, S2, …）。
func (w *graphTDWriter) nodeID(s Stager) string {
	if id, ok := w.ids[s]; ok {
		return id
	}
	w.seq++
	id := fmt.Sprintf("S%d", w.seq)
	w.ids[s] = id
	return id
}

// edge 输出一行 Mermaid 边：S1["name"] --> S2["name"]。
func (w *graphTDWriter) edge(from, to Stager) {
	fid, tid := w.nodeID(from), w.nodeID(to)
	_, err := fmt.Fprintf(w.b, "    %s[\"%s\"] --> %s[\"%s\"]\n", fid, from.Name(), tid, to.Name())
	if err != nil {
		return
	}
}

// StageConfig 描述单个 Stage 的运行时配置。
//
// OutCap 保存当前 Stage 输出 Channel 的容量（D-14）；
// 输入 Channel 由 NewStage 的 inputChan 参数注入（外部传入）。
type StageConfig struct {
	// Workers 为 WorkerPool 并发数，必须 ≥ 1。
	Workers int
	// OutCap 为当前 Stage 输出 Channel 容量，必须 ≥ 0。
	OutCap int
	// Timeout 为单条数据处理超时，0 表示不超时。
	Timeout time.Duration
	// SlowThreshold 为慢处理阈值：单条耗时超过即打印慢日志（D-22），0 表示不启用。
	SlowThreshold time.Duration
	// Hooks 为 process 生命周期钩子（D-24），嵌入 process 前后回调，
	// 用户可在此注入 trace / 审计日志 / 限流等横切关注点，无需修改 process 函数。
	Hooks StageHooks
	// ErrPolicy 为本 Stage 的错误策略（D-04）。非 nil 时覆盖全局策略，
	// nil 时使用 Pipeline.ErrorPolicy() 注入的全局策略。
	ErrPolicy *ErrPolicy
	// RateLimiter 为本 Stage 的限流器（D-29）。nil = 不限流（零开销）。
	// process 前调用限流器：Wait（背压式阻塞）或 Allow（丢弃式跳过）。
	RateLimiter *RateLimiter
}

// StageHooks 定义 process 执行前后的生命周期回调（D-24）。
//
// 所有回调均为可选（nil 时跳过）；回调内不可 panic，不可阻塞主流程。
// 若 OnBeforeProcess 返回的 ctx 非 nil，则 process 将使用该 ctx（而非原始 ctx）。
type StageHooks struct {
	// OnBeforeProcess 在每条数据调用 process 前执行。
	// 返回的 context 若非 nil 将替换 process 的 ctx 参数（可用于注入 trace span / 超时覆盖等）。
	OnBeforeProcess func(ctx context.Context, in any) context.Context

	// OnAfterProcess 在每条数据 process 返回后执行（无论成功或失败）。
	// out 为 process 返回值（失败时为零值），err 为 process 返回的错误（nil 表示成功），
	// latency 为单条处理耗时（含超时取消时间）。
	OnAfterProcess func(ctx context.Context, in any, out any, err error, latency time.Duration)
}

// Stage 是单节点业务处理单元：泛型 struct，处理逻辑以函数字段注入（D-11）。
//
// 该设计替代早期「接口 + BaseStage 内嵌」方案，运行期无接口派发，API 为函数型。
//
// 数据流（D-13/D-14/D-15）：Stage 通过私有内嵌 Channel 流通数据——
//   - input  输入 Channel：NewStage 注入（只读视图 <-chan，接收上游/外部数据），私有不可导出；
//   - output 输出 Channel：NewStage 内部创建（容量 = StageConfig.OutCap），私有不可导出；
//   - 外部经 InChan()/OutChan() 方向化访问；
//   - 启动：Start(ctx, params) 接入 Context 链（D-15，前驱传递 forwardCtx）；
//   - 退出：Close(drainTimeout) 关闭后通过 forwardCtx 通知后继（D-16）；
//   - 当前串行链路中实例化为 Stage[T, T]（输入输出同类型），
//     双类型参数保留异构能力，供未来 Fork/Join 或独立使用场景。
type Stage[T1, T2 any] struct {
	name    string                                       // Stage 名称，用于日志文件名与链路分析
	config  StageConfig                                  // 运行时配置
	process func(ctx context.Context, in T1) (T2, error) // 单条数据处理，由 WorkerPool 调用

	// input 输入 Channel：NewStage 注入（外部传入的只读视图）。私有，经 InChan() 访问。
	input <-chan T1
	// output 输出 Channel：NewStage 创建，容量 = StageConfig.OutCap。私有，经 OutChan() 访问。
	output chan T2
	// routeFunc 本 Stage 的路由条件（D-25）：父节点投递前检查，false 则跳过此分支。
	// nil（默认）= 放行所有数据。构造时由 NewStage / NextStage 的 routeFn 参数注入。
	routeFunc func(T1) bool
	// subStages 子 Stage 列表（递归 Start/Close 用）。
	subStages []Stager
	// dataSubs 仅 NextStage 创建的数据流子节点（D-28，拓扑校验/遍历用；Attach 不记录）。
	dataSubs []Stager
	// subOutChans 每个子 Stage 独立的输入 Channel（Fork Fan-out 广播，D-21）。
	// 每条产出数据被复制分发到所有 subOutChans，各分支独立处理。
	subOutChans []chan T2
	// fanoutQueues 每个子分支对应的有界转发队列（容量 = 子 OutCap）。
	// dispatcher 读 output 后投递到各队列，每分支一个固定转发 worker 从队列
	// 写 subOutChan——goroutine 总数恒定（1 + 分支数），避免每数据建 goroutine
	// 导致高吞吐下无界并发（内存爆炸）。
	fanoutQueues []chan T2
	// fanoutRouteFuncs 每个子分支的路由条件（D-25），与 fanoutQueues 一一对应。
	// nil = 放行所有数据到该分支；非 nil 则仅当 func(v) 返回 true 时投递。
	fanoutRouteFuncs []func(T2) bool
	// fanoutWG 广播转发协程（各分支转发 worker）的等待组。
	fanoutWG sync.WaitGroup
	// dispatcherWG dispatcher 广播协程（读 output → 投递队列）的等待组。
	dispatcherWG sync.WaitGroup

	// running 原子状态：false=未启动，true=已启动（Start 校验用，D-15）。
	running atomic.Bool
	// mu 保护 Start/Close 互斥：防止 Close 在 pool.start() 完成前关闭 output。
	mu sync.Mutex
	// ctx 本 Stage 的生命周期 Context（WithCancel 自 Start 传入的 ctx，D-15）。
	ctx context.Context
	// cancel 取消 ctx 的 CancelFunc（Close 时先取消，让 worker 退出再排空）。
	cancel context.CancelFunc
	// forwardCtx 派发给后一个 Stage 的 Context（WithCancel 自 ctx，D-15/D-16）。
	forwardCtx context.Context
	// forwardCancel 取消 forwardCtx 的 CancelFunc（Close 时触发，通知后继，D-16）。
	forwardCancel context.CancelFunc
	// pool 内部泛型工作池（Start 时创建）。
	pool *workerPool[T1, T2]
	// sink 可选：非 nil 时 process 结果直接交给 sink 消费（叶子防背压死锁，D-21）。
	sink func(ctx context.Context, v T2)
	// logger 每 Stage 独立日志文件（D-08，Start 时由 params 创建）。
	logger *StageLogger
	// stageMonitor 耗时与计数统计（D-02，Start 时由 params 创建）。
	stageMonitor *StageMonitor
	// errPol 错误策略（D-04，从 params 读取后注入 workerPool）。
	errPol *ErrPolicy
	// dlWriter 死信队列写入器（D-23，从 params 读取后注入 workerPool）。
	dlWriter *deadLetterWriter
	// dlOwnSink 标记死信 sink 是否由本 Stage 创建（自己创建的自己关闭；
	// 用户传入的共享 sink 由用户负责 Close）。
	dlOwnSink bool
}

// deadLetterWriter 是 workerPool 写出死信记录的轻量封装：
// 持有 sink 与输入序列化器，经 workerPool.dlWriter 注入。
type deadLetterWriter struct {
	sink    DeadLetterSink
	marshal func(v any) ([]byte, error)
}

// NewStage 创建 Stage。
// 参数依次为：名称、配置、输入 Channel（外部传入，只读视图）、路由条件（nil=放行所有）、处理函数。
// 输出 Channel 在内部创建，容量取 cfg.OutCap（负值归一为 0）。
func NewStage[T1, T2 any](name string, cfg StageConfig, inputChan <-chan T1, routeFn func(T1) bool, fn func(ctx context.Context, in T1) (T2, error)) *Stage[T1, T2] {
	outCap := max(cfg.OutCap, 0)
	return &Stage[T1, T2]{
		name:      name,
		config:    cfg,
		process:   fn,
		input:     inputChan,
		output:    make(chan T2, outCap),
		routeFunc: routeFn,
	}
}

// InChan 返回输入 Channel 的只读视图（上游连线 / 外部喂数据用）。
func (s *Stage[T1, T2]) InChan() <-chan T1 { return s.input }

// OutChan 返回输出 Channel 的写入视图（下游连线 / 外部取数用）。
func (s *Stage[T1, T2]) OutChan() chan<- T2 { return s.output }

// Sink 将本 Stage 的 process 结果交给消费函数（统计/落库）而非写 output channel。
// 适用于叶子 Stage：无下游消费者时防止 output 背压死锁（D-21）。sink 为 nil 恢复默认写 output。
func (s *Stage[T1, T2]) Sink(fn func(ctx context.Context, v T2)) *Stage[T1, T2] {
	s.sink = fn
	return s
}

// Attach 将 st 注册为本 Stage 的生命周期子节点（D-26）。
//
// 与 NextStage 不同：Attach 只把 st 追加到 subStages（Start/Close/Describe/GraphTD 递归遍历），
// **不创建 fanout 分支、不参与数据转发**——适用于 MergeNode 这类"消费本 Stage 输出、
// 但生命周期需要跟随本 Stage 启停"的外部节点。可被多个父 Stage 同时 Attach（各父分别启动/关闭它）。
func (s *Stage[T1, T2]) Attach(st Stager) {
	s.subStages = append(s.subStages, st)
}

// Name 返回 Stage 名称。
func (s *Stage[T1, T2]) Name() string { return s.name }

// Config 返回该 Stage 的运行时配置。
func (s *Stage[T1, T2]) Config() StageConfig { return s.config }

// Process 执行单条消息处理，转发到注入的 process 字段。
func (s *Stage[T1, T2]) Process(ctx context.Context, in T1) (T2, error) {
	return s.process(ctx, in)
}

// ForwardCtx 返回派发给后继 Stage 的 Context（Pipeline 启动链路时取用，D-15）。
// 前驱 Close 完成后取消此 Context，即通知后继开始关闭。
func (s *Stage[T1, T2]) ForwardCtx() context.Context { return s.forwardCtx }

func (s *Stage[T1, T2]) NextStage[T3 any](name string, cfg StageConfig, routeFn func(T2) bool, fn func(ctx context.Context, in T2) (T3, error)) *Stage[T2, T3] {
	// 为子 Stage 创建独立输入 Channel（容量 = 子 Stage 的 OutCap），
	// 并 Append 到 subOutChans——父产出数据将被广播复制到此处（D-21）。
	subIn := make(chan T2, max(cfg.OutCap, 0))
	subStage := NewStage[T2, T3](name, cfg, subIn, routeFn, fn)
	s.subStages = append(s.subStages, subStage)
	s.dataSubs = append(s.dataSubs, subStage) // D-28：数据流子节点（拓扑校验用）
	s.subOutChans = append(s.subOutChans, subIn)
	// 每个子分支配一个有界转发队列（固定 worker 消费 → 写 subIn）。
	subQueue := make(chan T2, max(cfg.OutCap, 0))
	s.fanoutQueues = append(s.fanoutQueues, subQueue)
	s.fanoutRouteFuncs = append(s.fanoutRouteFuncs, routeFn)
	return subStage
}

func (s *Stage[T1, T2]) Describe(parent string) {
	for _, stage := range s.subStages {
		stage.Describe(s.name)
	}
	fmt.Printf("%s -> %s\n", parent, s.name)
}

// GraphTD 将本 Stage 及其子 Stage 的链式关系写入 Mermaid graph TD 格式的 writer。
func (s *Stage[T1, T2]) GraphTD(w *graphTDWriter) {
	for _, stage := range s.subStages {
		w.edge(s, stage)
		stage.GraphTD(w)
	}
}

// Start 启动 Stage（D-15），接入 Context 链并启动 WorkerPool。
//
// ctx 为上游传入的 Context（Pipeline 启动时按从前往后顺序传入前驱的
// forwardCtx：s1.Start(root) → s2.Start(s1.ForwardCtx()) → …）。
// 内部以 context.WithCancel(ctx) 派生 forwardCtx 供后继 Stage 使用。
//
// params 为预留的参数入口（map[string]any），先在内部完成状态校验：
//   - input 非 nil，否则返回 ErrStageMissingInput；
//   - output 非 nil，否则返回 ErrStageMissingOutput；
//   - running 为 false（未重复启动），否则返回 ErrStageAlreadyRunning。
//
// 校验通过后：running 置 true，启动内部泛型 WorkerPool 监听 input。
// 后续扩展：从 params 读取超时覆盖、错误策略等运行时选项。
func (s *Stage[T1, T2]) Start(ctx context.Context, params map[string]any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.input == nil {
		return ErrStageMissingInput
	}
	if s.output == nil {
		return ErrStageMissingOutput
	}
	if s.running.Load() {
		return ErrStageAlreadyRunning
	}

	// 创建日志文件（D-08）：从 params 读取 logDir/logEnabled/logLevel。
	if params != nil {
		if dir, ok := params["logDir"].(string); ok && dir != "" {
			if enabled, ok := params["logEnabled"].(bool); ok && enabled {
				level := LogLevelInfo
				if lv, ok := params["logLevel"].(LogLevel); ok {
					level = lv
				}
				if l, err := NewStageLogger(s.name, dir, level); err == nil {
					s.logger = l
				}
			}
		}
		// 创建耗时监控（D-02）：从 params 读取 monitor 对象并注册。
		if mon, ok := params["monitor"].(*Monitor); ok && mon != nil {
			s.stageMonitor = &StageMonitor{depthFn: func() int { return len(s.input) }}
			mon.Register(s.name, s.stageMonitor)
		}
	}
	// 读取错误策略（D-04）：优先使用本 Stage 配置的 ErrPolicy（覆盖全局策略）。
	// 配置级策略不依赖 params，放在 params 块之外。
	if s.config.ErrPolicy != nil {
		s.errPol = s.config.ErrPolicy
	} else if params != nil {
		if ep, ok := params["errPol"].(*ErrPolicy); ok && ep != nil {
			s.errPol = ep
		}
	}
	// 读取死信队列配置（D-23）：从 params 读取 deadLetter 对象。
	if params != nil {
		if dl, ok := params["deadLetter"].(*DeadLetterConfig); ok && dl != nil {
			sink := dl.Sink
			own := false
			if sink == nil {
				dir := dl.Dir
				if dir == "" {
					dir = defaultDeadLetterDir(params)
				}
				sink = NewJSONLDeadLetterSink(dir)
				own = true
			}
			s.dlWriter = &deadLetterWriter{sink: sink, marshal: dl.MarshalInput}
			s.dlOwnSink = own
		}
	}
	// 1.启动子Stage（顺序启动，任一失败则返回错误，不继续启动父 Stage）。
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.forwardCtx, s.forwardCancel = context.WithCancel(s.ctx)
	for i, st := range s.subStages {
		if err := st.Start(s.forwardCtx, params); err != nil {
			// 清理已成功启动的子 Stage（避免 goroutine/channel 泄漏）。
			for j := 0; j < i; j++ {
				_ = s.subStages[j].Close(0) // 0=drainTimeout 立即关闭，不等待排空
			}
			return err
		}
	}

	// 启动自己
	s.pool = newWorkerPool(s.config.Workers, s.input, s.output, s.process, s.config.Timeout, nil)
	s.pool.sink = s.sink
	s.pool.stageName = s.name
	s.pool.slowThreshold = s.config.SlowThreshold
	s.pool.stageMonitor = s.stageMonitor
	s.pool.errPol = s.errPol
	s.pool.hooks = &s.config.Hooks
	s.pool.dlWriter = s.dlWriter
	s.pool.rateLimiter = s.config.RateLimiter
	s.pool.cancel = s.forwardCancel
	s.running.Store(true)
	s.pool.start(s.ctx)

	// 启动广播转发：从 output 读出每条数据，复制分发到每个子 Stage 的独立通道。
	if len(s.subOutChans) > 0 {
		s.startFanout()
	}
	if s.logger != nil {
		s.logger.Infow("stage started", F("stage", s.name))
	}
	return nil
}

// startFanout 启动广播转发（D-21，修复无界 goroutine 缺陷）。
//
// 架构：1 个 dispatcher goroutine 从 s.output 读每条数据，投递到每个子分支的
// 有界队列 fanoutQueues；每个子分支 1 个常驻转发 worker 从队列取数据写入
// 对应 subOutChan。goroutine 总数恒定（1 + 分支数），不随数据量增长——
// 高吞吐下不会像「每数据一 goroutine」那样堆积导致内存爆炸。
//
// 投递含超时（s.config.Timeout，0 表示不超时）：
//   - dispatcher → 队列：队列已满且超时则丢弃这本该发给慢分支的副本，不阻塞其他分支；
//   - 转发 worker → subOutChan：子 Stage 消费慢且超时则放弃该条，不拖累后续数据。
func (s *Stage[T1, T2]) startFanout() {
	// 每个子分支一个固定转发 worker。
	for i := range s.fanoutQueues {
		out := s.subOutChans[i]
		queue := s.fanoutQueues[i]
		s.fanoutWG.Add(1)
		go func() {
			defer s.fanoutWG.Done()
			for v := range queue {
				s.fanoutSend(out, v)
			}
		}()
	}
	// dispatcher：读 output → 投递到各分支队列。
	// 结束后关闭所有 fanoutQueues（dispatcher 是唯一写入方），触发 workers 排空退出。
	s.dispatcherWG.Add(1)
	go func() {
		defer s.dispatcherWG.Done()
		defer func() {
			for _, queue := range s.fanoutQueues {
				close(queue)
			}
		}()
		for v := range s.output {
			for i, queue := range s.fanoutQueues {
				// 路由条件检查（D-25）：routeFn 非 nil 且返回 false 时跳过此分支。
				if s.fanoutRouteFuncs[i] != nil && !s.fanoutRouteFuncs[i](v) {
					continue
				}
				s.fanoutSend(queue, v)
			}
		}
	}()
}

// fanoutSend 将单条数据发送到指定通道，防单分支阻塞（带超时）。
func (s *Stage[T1, T2]) fanoutSend(ch chan T2, v T2) {
	if s.config.Timeout > 0 {
		timer := time.NewTimer(s.config.Timeout)
		defer timer.Stop()
		select {
		case ch <- v:
		case <-s.ctx.Done():
		case <-timer.C:
		}
		return
	}
	select {
	case ch <- v:
	case <-s.ctx.Done():
	}
}

// Close 优雅关闭 Stage（D-16）。
//
// 流程：等待 WorkerPool 排空在途数据（受 drainTimeout 约束，超时返回
// ErrCloseTimeout）→ 关闭 output Channel（数据级关闭信号）→ 取消 forwardCtx
// （通知后继 Stage，前驱关闭完成后逐级传递到最后一个 Stage）→ running 复位。
// 幂等：running 已复位时调用直接返回 nil。
func (s *Stage[T1, T2]) Close(drainTimeout time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running.Load() {
		return nil
	}

	// 1.关闭自己：先等自然排空（drainTimeout），超时后才强制取消 ctx 防死锁。
	done := make(chan struct{})
	go func() {
		s.pool.wait()
		close(done)
	}()

	drainTimer := time.NewTimer(drainTimeout)
	defer drainTimer.Stop()
	select {
	case <-done:
		// 自然排空完成
	case <-drainTimer.C:
		// 排空超时：强制取消 ctx 让 worker 退出（防 output 满阻塞死锁），再等一次。
		if s.cancel != nil {
			s.cancel()
		}
		secondTimer := time.NewTimer(drainTimeout)
		select {
		case <-done:
		case <-secondTimer.C:
			secondTimer.Stop()
			// 兜底清理：即使超时也要取消 forwardCtx 通知后继、复位 running，
			// 确保幂等和子 Stage 关闭。注：不关闭 s.output——
			// worker 在 ctx 取消后经 select { case wp.out<-out; case <-ctx.Done() }
			// 自然退出，关闭 output 会导致仍在运行的 worker panic。
			if s.forwardCancel != nil {
				s.forwardCancel()
			}
			s.running.Store(false)
			return ErrCloseTimeout
		}
		secondTimer.Stop()
	}

	close(s.output)
	// 广播转发关闭顺序（D-21）：
	//   1. 等 dispatcher 读完 output 剩余数据，并关闭各 fanoutQueues；
	//   2. 等各分支转发 worker 排空队列、写入 subOutChan 完成；
	//   3. 最后关闭 subOutChans（避免向已关闭 Channel 发送导致 panic）。
	if len(s.subOutChans) > 0 {
		s.dispatcherWG.Wait()
		s.fanoutWG.Wait()
		for _, ch := range s.subOutChans {
			close(ch)
		}
	}
	if s.forwardCancel != nil {
		s.forwardCancel()
	}
	s.running.Store(false)
	// 关闭日志：写关闭事件 + 关闭日志文件。
	if s.logger != nil {
		s.logger.Infow("stage closed", F("stage", s.name))
		_ = s.logger.Close()
	}
	// 关闭自己创建的死信 sink（用户传入的共享 sink 由用户负责关闭）。
	if s.dlWriter != nil && s.dlOwnSink {
		if err := s.dlWriter.sink.Close(); err != nil {
			if s.logger != nil {
				s.logger.Errorw("dead letter sink close error", F("stage", s.name), F("error", err))
			}
		}
		s.dlWriter = nil
	}
	// 关闭子Stage
	var closeErr error
	for _, st := range s.subStages {
		if err := st.Close(drainTimeout); err != nil && closeErr == nil {
			closeErr = err
			if s.logger != nil {
				s.logger.Errorw("subStage close error", F("stage", s.name), F("subStage", st.Name()), F("error", err))
			}
		}
	}
	return closeErr
}
