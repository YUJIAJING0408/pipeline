package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Keyable 表示可提取合并键的类型（D-26）。
//
// MergeKey 返回合并分组键（订单 ID / 用户 ID / traceID 等业务已有字段）。
// 仅 MergeNode 参与的分支输出类型需要实现，不约束现有 Stage 的泛型类型。
type Keyable interface {
	MergeKey() string
}

// ErrJoinSizeMismatch 表示 Wire 的分支数 ≠ JoinConfig.Size。
var ErrJoinSizeMismatch = errors.New("pipeline: merge node wired branches mismatch config.Size")

// ErrJoinConfig 表示 MergeNode 配置非法。
var ErrJoinConfig = errors.New("pipeline: invalid merge node config")

// JoinConfig 描述 MergeNode 的运行配置（D-26）。
// T 为合并输出类型（受 Keyable 约束）。
type JoinConfig[T Keyable] struct {
	// Size 每批分支数（必须 == Wire 的分支数，否则 Start 返回 ErrJoinSizeMismatch）。
	Size int
	// MergeTimeout 同 key 等待超时：超过仍未凑齐视为泄漏（sweep 清理）。
	// 0 表示不启用过期清理。
	MergeTimeout time.Duration
	// SweepInterval 过期扫描间隔；0 时默认 15s。仅 MergeTimeout > 0 时生效。
	SweepInterval time.Duration
	// DeadLetter 泄漏残批的落盘实现（可选；nil = 仅 OnLeak 回调后删除）。
	// 由用户负责 Close（与 Stage 共享 sink 语义一致）。
	DeadLetter DeadLetterSink
	// OnLeak 残批（超时未凑齐 / Close 时不足 Size）的回调，用于告警/计数。
	// 被调用后该批从 map 删除。nil = 跳过。
	OnLeak func(key string, batch []T)
	// MergeWorkers 合并工作池并发数；0 = 1。
	MergeWorkers int
	// MergeQueueCap 合并任务队列容量；0 = 64。
	MergeQueueCap int
	// OutCap 合并结果输出通道容量；0 = 64。
	OutCap int
	// SlowThreshold merge 耗时阈值：单次合并超过即打印慢日志（D-22 对齐，0 = 不启用）。
	SlowThreshold time.Duration
	// RateLimiter merge 前限流器（D-29 对齐，nil = 不限流）。
	// Wait 背压式阻塞等待令牌；ctx 取消时该批次放弃。
	RateLimiter *RateLimiter
	// OnMergeError merge 失败回调（D-04 对齐，merge 返回错误或 panic 时触发）。
	// 被调用后该批次丢弃（不进入下游）。nil = 跳过。
	OnMergeError func(err error, batch []T)
}

// pending 是单个 key 的凑齐状态。
// branchArr 位图语义：每分支最多记 1 条，arrivedCnt == Size 即凑齐
// （防止同 key 多批次时 len(datas)==Size 的误判）。
type pending[T any] struct {
	datas      []T       // 按分支序号存放已到达结果
	branchArr  []bool    // 各分支是否已到达
	arrivedCnt int       // 已到达分支数
	updateTime time.Time // 最后到达时间（过期扫描依据）
}

// collectItem 是分支转发 goroutine → 收集 goroutine 的单条投递。
type collectItem[T any] struct {
	v   T
	idx int // 分支序号
}

// MergeNode 是条件汇聚（Keyed Fan-in）节点（D-26）。
//
// 消费多个分支（Wire 接入）的输出，按 MergeKey 分组，同一 key 凑齐
// JoinConfig.Size 个分支结果后整批送合并工作池；合并结果写 output 供下游消费。
//
// 并发模型：单收集 goroutine 独占 pending map（零锁）；每个分支一个转发
// goroutine 写收集通道；合并计算走独立工作池（不阻塞收集）。
//
// 生命周期：经标注的父 Stage 通过 Attach 注册；Start/Close 由父树递归驱动。
// 由于可能被多个父 Attach，启动用 sync.Once 幂等，关闭用引用计数——
// 真正关闭发生在最后一个父关闭时（此时全部分支已停，收集自然退出）。
type MergeNode[T Keyable] struct {
	name   string
	cfg    JoinConfig[T]
	merge  func(ctx context.Context, batch []T) (T, error)
	srcs   []srcEntry[T]         // Wire 采集的分支（Stager 用于拓扑 + channel 用于收集）
	output chan T                // 合并结果内部通道（fan-out 分发到各下游）
	outs   []outEntry[T]         // 下游分支（路由 + 生命周期 + 拓扑）

	pending   map[string]*pending[T]
	collector chan collectItem[T] // 转发 goroutine → 收集 goroutine
	mergeCh   chan []T            // 收集 → merge 工作池的有界任务队列

	srcWG sync.WaitGroup  // 各分支转发 goroutine
	mgWG  sync.WaitGroup  // merge 工作池
	cgWG  sync.WaitGroup  // 收集 goroutine
	fanWG sync.WaitGroup  // fan-out 分发 goroutine

	// stageMonitor 耗时与计数统计（D-30，params 注入，非 nil 时记录 merge 数据）。
	stageMonitor *StageMonitor

	ctx    context.Context
	cancel context.CancelFunc

	startOnce  sync.Once
	startErr   error
	closeOnce  sync.Once
	closeErr   error
	refs       atomic.Int32  // 当前存活的父调用方计数（Start +1 / Close -1）
	describeOnce sync.Once

	// subStages 下游 Stage 列表（由 NextStage 创建，Start/Close 递归遍历）。
	subStages []Stager
}

// outEntry 记录一个下游分支（fan-out 目标 + 路由条件 + 生命周期引用）。
type outEntry[T any] struct {
	ch      chan T      // 下游分支独立输入通道（fan-out 复制目标）
	stage   Stager      // 下游 Stage（生命周期 + 拓扑）
	routeFn func(T) bool // 路由条件（nil = 放行所有）
}

// srcEntry 记录一个 Wire 接入的分支（channel 元素类型 T）。
type srcEntry[T any] struct {
	stage Stager
	ch    <-chan T
}

// RenderGraph 用同一个 graphTDWriter 渲染多个根节点的拓扑，节点 ID 全局唯一。
//
// 适用场景：root 树（Pipeline.GraphTD 只画自身子树）与 MergeNode 段（并入的多分支）
// 是两个独立的根，需合并成一张完整图：
//
//	root 树:   S1[root] --> S2[branch-inventory]
//	merge 段:  S2[branch-inventory] --> S3[merge-settle] --> S4[sink-audit]
//
// 因 MergeNode 与 root 树中的分支属于同一张图，传入顺序保证渲染完整：
//
//	RenderGraph(root, merge, sink)
func RenderGraph(roots ...Stager) string {
	var b strings.Builder
	b.WriteString("graph TD\n")
	w := &graphTDWriter{b: &b, ids: make(map[Stager]string)}
	for _, r := range roots {
		if r == nil {
			continue
		}
		r.GraphTD(w)
	}
	return b.String()
}

// NewMergeNode 创建合并节点。
//
// merge 将一批（JoinConfig.Size 条、同 MergeKey）结果合并为 1 条输出。
// T 受 Keyable 约束：分支输出类型未实现 MergeKey 时编译期即失败。
func NewMergeNode[T Keyable](name string, cfg JoinConfig[T],
	merge func(ctx context.Context, batch []T) (T, error)) *MergeNode[T] {
	if cfg.Size < 1 {
		cfg.Size = 1
	}
	if cfg.MergeWorkers < 1 {
		cfg.MergeWorkers = 1
	}
	if cfg.MergeQueueCap < 1 {
		cfg.MergeQueueCap = 64
	}
	if cfg.OutCap < 1 {
		cfg.OutCap = 64
	}
	return &MergeNode[T]{
		name:      name,
		cfg:       cfg,
		merge:     merge,
		output:    make(chan T, cfg.OutCap),
		pending:   make(map[string]*pending[T]),
		collector: make(chan collectItem[T]),
		mergeCh:   make(chan []T, cfg.MergeQueueCap),
	}
}

// NextStage 创建下游 Stage，自动接入 merge 输出并纳入生命周期管理。
//
// 与手动 NewStage(merge.InChan()) 的区别：通过 NextStage 创建的下游 Stage
// 自动注册到 MergeNode 的 subStages，Start/Close 由 MergeNode 递归遍历，
// 用户无需手动管理其生命周期（MergeNode 已通过 Attach 挂到父树）。
//
// 可多次调用 NextStage 创建多个下游分支，每个分支独立接受 merge 输出（fan-out 复制）。
// routeFn 非 nil 时仅当函数返回 true 才将合并结果投递到该分支（条件路由，D-25）。
func (j *MergeNode[T]) NextStage[T3 any](name string, cfg StageConfig, routeFn func(T) bool, fn func(ctx context.Context, in T) (T3, error)) *Stage[T, T3] {
	ch := make(chan T, max(cfg.OutCap, 0))
	downstream := NewStage(name, cfg, ch, nil, fn)
	j.outs = append(j.outs, outEntry[T]{ch: ch, stage: downstream, routeFn: routeFn})
	j.subStages = append(j.subStages, downstream)
	return downstream
}

// Wire 接入一个分支：MergeNode 将消费 s 的输出。
func (j *MergeNode[T]) Wire[In any](s *Stage[In, T]) *MergeNode[T] {
	j.srcs = append(j.srcs, srcEntry[T]{stage: s, ch: s.output})
	return j
}

// To 登记下游 Stage（仅拓扑接线，用于 GraphTD 画 merge→downstream 边）。
// 与 NextStage 不同：To 不创建新通道，不参与数据流和生命周期管理。
// 数据连接由调用方通过 j.InChan() 手动完成。
// 仅当 NextStage 无法满足接线需求时使用。
func (j *MergeNode[T]) To(downstream Stager) *MergeNode[T] {
	j.outs = append(j.outs, outEntry[T]{stage: downstream})
	return j
}

// InChan 返回合并结果的输出通道视图（供下游 NewStage 作为 input）。
func (j *MergeNode[T]) InChan() <-chan T { return j.output }

// Name 返回节点名称。
func (j *MergeNode[T]) Name() string { return j.name }

// Described 状态由 describeOnce 保护（多父重复调用只打印一次）。

// Describe 打印合并节点拓扑（由父树递归驱动；once 去重多父重复打印）。
func (j *MergeNode[T]) Describe(parent string) {
	j.describeOnce.Do(func() {
		names := make([]string, 0, len(j.srcs))
		for _, s := range j.srcs {
			names = append(names, s.stage.Name())
		}
		fmt.Printf("%s -> %s ( merge of [%s] )\n", parent, j.name, strings.Join(names, ", "))
		for _, o := range j.outs {
			if o.stage != nil {
				o.stage.Describe(j.name)
			}
		}
	})
}

// GraphTD 将合并段下游拓扑写入 Mermaid graph TD writer。
//
// 注意：MergeNode 已通过 Attach 注册到各分支的 subStages，分支递归 GraphTD
// 时会自然画出 fan-in 入边（branch-A --> merge / branch-B --> merge / …）。
// 因此本方法只负责画下游（NextStage/To 登记的各分支）。
// 用下游是否已入图去重，避免重复边。
func (j *MergeNode[T]) GraphTD(w *graphTDWriter) {
	for _, o := range j.outs {
		if o.stage == nil {
			continue
		}
		if _, drawn := w.ids[o.stage]; !drawn {
			w.edge(j, o.stage)
		}
		if g, ok := o.stage.(interface{ GraphTD(*graphTDWriter) }); ok {
			g.GraphTD(w)
		}
	}
}

// Start 启动合并节点。可能被多个父 Stage 递归调用：
// doStart 仅执行一次（startOnce），其余调用直接返回首次结果。
func (j *MergeNode[T]) Start(ctx context.Context, params map[string]any) error {
	j.refs.Add(1)
	j.startOnce.Do(func() { j.startErr = j.doStart(ctx, params) })
	return j.startErr
}

// doStart 校验配置并启动转发 goroutine / 收集 goroutine / merge 工作池。
func (j *MergeNode[T]) doStart(ctx context.Context, params map[string]any) error {
	if len(j.srcs) != j.cfg.Size {
		return fmt.Errorf("%w: wired=%d size=%d", ErrJoinSizeMismatch, len(j.srcs), j.cfg.Size)
	}
	// 校验分支不重复。
	seen := make(map[Stager]bool, len(j.srcs))
	for _, s := range j.srcs {
		if seen[s.stage] {
			return fmt.Errorf("%w: duplicate branch %q", ErrJoinConfig, s.stage.Name())
		}
		seen[s.stage] = true
	}

	// 注册监控统计（D-30）：从 params 读取 monitor（与 Stage 对齐）。
	if params != nil {
		if mon, ok := params["monitor"].(*Monitor); ok && mon != nil {
			j.stageMonitor = &StageMonitor{}
			mon.Register(j.name, j.stageMonitor)
		}
	}

	j.ctx, j.cancel = context.WithCancel(ctx)

	// 分支转发 goroutine：源 channel 关闭后自然退出，写收集通道可被 ctx 打断。
	for i, s := range j.srcs {
		j.srcWG.Add(1)
		go func(ch <-chan T, idx int) {
			defer j.srcWG.Done()
			for {
				select {
				case <-j.ctx.Done():
					return
				case v, ok := <-ch:
					if !ok {
						return // 分支已关闭
					}
					select {
					case j.collector <- collectItem[T]{v: v, idx: idx}:
					case <-j.ctx.Done():
						return
					}
				}
			}
		}(s.ch, i)
	}

	// merge 工作池：消费 mergeCh，执行合并后写 output。
	for i := 0; i < j.cfg.MergeWorkers; i++ {
		j.mgWG.Add(1)
		go func() {
			defer j.mgWG.Done()
			for batch := range j.mergeCh {
				j.runMerge(batch)
			}
		}()
	}

	// 收集 goroutine：独占 pending map（零锁）。
	j.cgWG.Add(1)
	go func() {
		defer j.cgWG.Done()
		var sweepC <-chan time.Time
		if j.cfg.MergeTimeout > 0 {
			interval := j.cfg.SweepInterval
			if interval <= 0 {
				// 默认扫描间隔不超过 MergeTimeout 的一半，避免泄漏清理滞后。
				interval = j.cfg.MergeTimeout / 2
				if min := 10 * time.Millisecond; interval < min {
					interval = min
				}
			}
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			sweepC = ticker.C
		}
		for {
			select {
			case <-j.ctx.Done():
				j.flushAll(true)
				return
			case <-sweepC:
				j.sweepExpired()
			case item, ok := <-j.collector:
				if !ok {
					j.flushAll(true)
					return
				}
				j.handleCollect(item)
			}
		}
	}()
	// 启动下游 subStages（NextStage 创建的下游 Stage，merge 输出已就绪）。
	for _, st := range j.subStages {
		if err := st.Start(j.ctx, nil); err != nil {
			return err
		}
	}
	// 启动 fan-out 分发 goroutine：从 j.output 读每条合并结果，按 routeFn 分发到各下游分支。
	if len(j.outs) > 0 {
		j.fanWG.Add(1)
		go func() {
			defer j.fanWG.Done()
			for v := range j.output {
				for _, o := range j.outs {
					if o.routeFn != nil && !o.routeFn(v) {
						continue // 路由拒绝，跳过此分支
					}
					if o.ch != nil {
						select {
						case o.ch <- v:
						case <-j.ctx.Done():
							return
						}
					}
				}
			}
		}()
	}
	return nil
}

// handleCollect 处理一条到达数据：定位/新建 pending，凑齐则投递合并。
// 仅由收集 goroutine 调用（单 goroutine 独占 pending，无需锁）。
func (j *MergeNode[T]) handleCollect(item collectItem[T]) {
	key := item.v.MergeKey()
	p, ok := j.pending[key]
	if !ok {
		p = &pending[T]{
			datas:     make([]T, len(j.srcs)),
			branchArr: make([]bool, len(j.srcs)),
		}
		j.pending[key] = p
	}
	// 分支幂等：同分支同 key 第 2 条忽略。
	if item.idx < len(p.branchArr) && p.branchArr[item.idx] {
		return
	}
	p.datas[item.idx] = item.v
	p.branchArr[item.idx] = true
	p.arrivedCnt++
	p.updateTime = time.Now()

	if p.arrivedCnt == j.cfg.Size {
		batch := make([]T, j.cfg.Size)
		copy(batch, p.datas)
		delete(j.pending, key)
		j.enqueue(batch, p)
	}
}

// enqueue 将完成批次投递到合并队列。
// 阻塞式投递：merge 工作池持续消费 mergeCh，不会死锁；
// 加 ctx.Done() 保护，防止极端背压下关闭时永久阻塞（关闭时会 cancel ctx 唤醒本方法）。
func (j *MergeNode[T]) enqueue(batch []T, p *pending[T]) {
	select {
	case j.mergeCh <- batch:
	case <-j.ctx.Done():
	}
}

// runMerge 执行一次合并并写 output（merge 工作池 goroutine）。
// 支持限流（D-29）/ 慢日志（D-22）/ 错误回调（D-04）/ 监控统计（D-02），与 Stage 对齐（D-30）。
// merge panic 被 recover 捕获并经 OnMergeError 回调，该批次丢弃（不崩溃）。
func (j *MergeNode[T]) runMerge(batch []T) {
	start := time.Now()

	// 限流（D-29）：merge 前取得令牌，Wait 背压式阻塞；ctx 取消时放弃该批次。
	if j.cfg.RateLimiter != nil {
		if err := j.cfg.RateLimiter.Wait(j.ctx); err != nil {
			return // ctx 取消或等待中断：放弃该批次
		}
	}

	var out T
	var merr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				merr = fmt.Errorf("panic in merge: %v\n%s", r, debug.Stack())
			}
		}()
		out, merr = j.merge(j.ctx, batch)
	}()

	elapsed := time.Since(start)

	// 慢日志（D-22）：单次合并超过阈值即打印。
	if j.cfg.SlowThreshold > 0 && elapsed > j.cfg.SlowThreshold {
		fmt.Printf("[slow] stage=%s took=%v threshold=%v batch_size=%d\n",
			j.name, elapsed, j.cfg.SlowThreshold, len(batch))
	}

	// 监控统计（D-02）：记录 merge 耗时与成败。
	if j.stageMonitor != nil {
		j.stageMonitor.record(elapsed, merr != nil)
	}

	// 错误处理（D-04）：merge 失败触发回调，批次丢弃。
	if merr != nil {
		if j.cfg.OnMergeError != nil {
			j.cfg.OnMergeError(merr, batch)
		}
		return
	}

	select {
	case j.output <- out:
	case <-j.ctx.Done():
	}
}

// sweepExpired 清理超时未凑齐的 key（残批 → OnLeak + 死信 + 删除）。
// 仅收集 goroutine 调用。
func (j *MergeNode[T]) sweepExpired() {
	now := time.Now()
	for key, p := range j.pending {
		if now.Sub(p.updateTime) > j.cfg.MergeTimeout {
			j.releaseLeak(key, p)
			delete(j.pending, key)
		}
	}
}

// flushAll 在收集 goroutine 退出前将全部残留 key 交付（残批语义，不丢失）。
// 仅收集 goroutine 调用。
func (j *MergeNode[T]) flushAll(_ bool) {
	for key, p := range j.pending {
		j.releaseLeak(key, p)
		delete(j.pending, key)
	}
}

// releaseLeak 交付一个残批（不足 Size）：OnLeak 回调 + 可选死信。
func (j *MergeNode[T]) releaseLeak(key string, p *pending[T]) {
	// 已到达的分支数据收集为切片。
	var batch []T
	for i, arrived := range p.branchArr {
		if arrived {
			batch = append(batch, p.datas[i])
		}
	}
	if j.cfg.OnLeak != nil {
		j.cfg.OnLeak(key, batch)
	}
	if j.cfg.DeadLetter != nil {
		// 尽量序列化：无法序列化的字段降级为 fmt 文本。
		rec := DeadLetterRecord{
			TS:    time.Now(),
			Stage: j.name,
			ErrMsg: fmt.Sprintf("merge timeout or incomplete batch: key=%s arrived=%d/%d",
				key, len(batch), j.cfg.Size),
		}
		if len(batch) > 0 {
			if b, err := json.Marshal(batch); err == nil {
				rec.Input = b
			} else {
				rec.Input = []byte(fmt.Sprintf("%+v", batch))
			}
		}
		_ = j.cfg.DeadLetter.Write(rec) // 死信写失败不阻塞
	}
}

// Close 关闭合并节点。由各父 Stage 递归调用：
// 引用计数递减，最后一个父关闭时（refs==0）才真正关闭——
// 此时所有分支已停（源 channel 已闭）、收集自然退出，无阻塞风险。
func (j *MergeNode[T]) Close(drainTimeout time.Duration) error {
	if j.refs.Add(-1) > 0 {
		return nil
	}
	j.closeOnce.Do(func() {
		j.closeErr = j.doClose(drainTimeout)
	})
	return j.closeErr
}

// doClose 真正关闭：停止收集 → 排空合并队列 → 关闭 output。
func (j *MergeNode[T]) doClose(drainTimeout time.Duration) error {
	if j.cancel != nil {
		j.cancel()
	}
	done := make(chan struct{})
	go func() {
		j.srcWG.Wait()  // 转发 goroutine 退出（分支已关闭 or ctx 取消）
		j.cgWG.Wait()   // 收集 goroutine 退出（含 flushAll 交付残余）
		close(j.mergeCh) // 唤醒 merge 工作池排空
		j.mgWG.Wait()   // merge 工作池排空完成
		close(done)
	}()
	drainTimer := time.NewTimer(drainTimeout)
	defer drainTimer.Stop()
	select {
	case <-done:
	case <-drainTimer.C:
		return ErrCloseTimeout
	}
	close(j.output)
	// 等待 fan-out 分发 goroutine 退出（j.output 已闭，range 循环自然结束）。
	j.fanWG.Wait()
	// 关闭所有下游分支的输入通道，通知下游 Stage 排空。
	for _, o := range j.outs {
		if o.ch != nil {
			close(o.ch)
		}
	}
	// 关闭下游 subStages（NextStage 创建的下游 Stage，此时各分支输入通道已闭，
	// 下游 Stage 的 worker 自然排空，不会阻塞）。
	for i := len(j.subStages) - 1; i >= 0; i-- {
		_ = j.subStages[i].Close(drainTimeout)
	}
	return nil
}