package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// workerPool 是 Stage 内部的数据处理工作池（泛型，D-15）。
//
// 语义：
//   - 启动后并发监听 Input Channel（worker 数 = StageConfig.Workers）；
//   - 收到一条数据 → 执行 process(ctx, in) → 若配置了 sink 则交给 sink 消费，
//     否则写入 Output Channel（D-21）；
//   - 单条处理受 timeout 约束（0 表示不超时），超时返回 ctx.Err() 触发 onError；
//   - 单条处理计时：耗时超过 slowThreshold 即打印慢日志（D-22），0 表示不启用；
//   - 处理出错时调用 onError 回调，该条数据不进入下游（跳过）；
//   - ctx 取消（前驱 Close 触发 forwardCtx 取消）：worker 停止接收与写出并退出；
//   - Input Channel 关闭（级联关闭信号）：各 worker 排空后自然退出。
type workerPool[In, Out any] struct {
	workers int
	in      <-chan In
	out     chan<- Out
	process func(ctx context.Context, in In) (Out, error)
	timeout time.Duration
	onError func(err error, in In)
	// sink 非 nil 时 process 结果直接交给 sink 消费，不写 out（叶子防背压死锁）。
	sink func(ctx context.Context, v Out)
	// stageName 用于慢日志标识。
	stageName string
	// slowThreshold 慢处理阈值：单条耗时超过即打印，0 表示不启用（D-22）。
	slowThreshold time.Duration
	// stageMonitor 耗时与计数统计（D-02，非 nil 时记录）。
	stageMonitor *StageMonitor
	// logger 结构化日志写入器（非 nil 时慢日志走 Warnw 落日志文件，否则走 stdout）。
	logger *StageLogger
	// errPol 错误策略（D-04，非 nil 时按模式处理错误）。
	errPol *ErrPolicy
	// hooks 生命周期回调（D-24，非 nil 时在 process 前后调用）。
	hooks *StageHooks
	// dlWriter 死信队列写入器（D-23，非 nil 时失败数据落死信）。
	dlWriter *deadLetterWriter
	// rateLimiter 限流器（D-29，非 nil 时 process 前 Wait 限流）。
	rateLimiter *RateLimiter
	// cancel 取消 Stage 上下文的函数（FailFast 模式调用）。
	cancel func()

	wg sync.WaitGroup
}

// newWorkerPool 创建工作池。workers < 1 时归一为 1。
func newWorkerPool[In, Out any](workers int, in <-chan In, out chan<- Out,
	process func(ctx context.Context, in In) (Out, error), timeout time.Duration,
	onError func(err error, in In)) *workerPool[In, Out] {
	if workers < 1 {
		workers = 1
	}
	return &workerPool[In, Out]{
		workers: workers,
		in:      in,
		out:     out,
		process: process,
		timeout: timeout,
		onError: onError,
	}
}

// start 启动 N 个 worker 协程消费输入。
func (wp *workerPool[In, Out]) start(ctx context.Context) {
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.run(ctx)
	}
}

// run 单个 worker 的消费循环。
func (wp *workerPool[In, Out]) run(ctx context.Context) {
	defer wp.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return // 前驱已关闭：停止接收与写出
		case in, ok := <-wp.in:
			if !ok {
				return // 输入关闭：排空完成，自然退出
			}
			wp.handle(ctx, in)
		}
	}
}

// handle 处理单条数据：执行 process（受超时约束）并写入输出。
// 处理前后计时，耗时超过 slowThreshold 即打印慢日志（D-22，与成功/失败无关）。
func (wp *workerPool[In, Out]) handle(ctx context.Context, in In) {
	start := time.Now()
	procCtx := ctx
	var cancel context.CancelFunc
	if wp.timeout > 0 {
		procCtx, cancel = context.WithTimeout(ctx, wp.timeout)
		defer cancel()
	}

	var out Out
	var err error

	// 生命周期钩子：OnBeforeProcess（D-24），在 timeout 之上覆盖 ctx。
	if wp.hooks != nil && wp.hooks.OnBeforeProcess != nil {
		if c := wp.hooks.OnBeforeProcess(ctx, in); c != nil {
			procCtx = c
		}
	}

	// 限流（D-29）：process 前取得令牌，Wait 背压式阻塞；ctx 取消时放弃该条。
	if wp.rateLimiter != nil {
		if err := wp.rateLimiter.Wait(procCtx); err != nil {
			return // ctx 取消或等待中断：放弃处理该条
		}
	}

	// 捕获 process 内部 panic，转换为 error 走正常错误处理路径
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in process: %v\n%s", r, debug.Stack())
			}
		}()
		out, err = wp.process(procCtx, in)
	}()

	elapsed := time.Since(start)

	// 生命周期钩子：OnAfterProcess（D-24）。
	if wp.hooks != nil && wp.hooks.OnAfterProcess != nil {
		wp.hooks.OnAfterProcess(ctx, in, out, err, elapsed)
	}

	if wp.slowThreshold > 0 && elapsed > wp.slowThreshold {
		if wp.logger != nil {
			wp.logger.Warnw("slow", F("stage", wp.stageName), F("input", in), F("took", elapsed.String()), F("threshold", wp.slowThreshold.String()))
		} else {
			fmt.Printf("[slow] stage=%s input=%v took=%v threshold=%v\n",
				wp.stageName, in, elapsed, wp.slowThreshold)
		}
	}
	// 记录耗时到监控统计（D-02）。注意：成败标记（err != nil）在错误处理完成后才记录，
	// 避免重试成功的数据被错误计入失败计数——首次失败但重试成功的数据，不计入 errors。
	// 监控统计在 success 标签和各失败 return 处分别记录。

	if err != nil {
		// 统一包装为 StageError（携带 Stage 名 / 输入值 / 耗时 / 错误分类），
		// 后续所有回调与策略决策都基于包装后的错误。
		se := wrapStageError(wp.stageName, in, err, elapsed)

		// 统一错误回调（D-04，所有模式触发）。
		if wp.onError != nil {
			wp.onError(se, in)
		}
		if wp.errPol != nil {
			if wp.errPol.OnError != nil {
				wp.errPol.OnError(se, wp.stageName, in)
			}
			switch wp.errPol.Mode {
			case ErrModeFailFast:
				wp.dlEnqueue(se, 0)
				if wp.stageMonitor != nil {
					wp.stageMonitor.record(time.Since(start), true)
					wp.stageMonitor.recordCode(se.Code)
				}
				if wp.cancel != nil {
					wp.cancel()
				}
				return // 处理失败，该条数据不进入下游
			case ErrModeRetryFallback:
				// 重试：按 MaxRetry 重试，间隔按指数退避（D-31）：
				// 第 n 次间隔 = RetryDelay × Backoff^(n-1)；Backoff ≤ 1 时固定 RetryDelay。
				// 每次重试使用新的超时上下文（避免首次超时后所有重试立即失败）。
				retryTimer := time.NewTimer(wp.retryDelay(0))
				retried := 0
				for i := 0; i < wp.errPol.MaxRetry; i++ {
					delay := wp.retryDelay(retried)
					retryTimer.Reset(delay)
					select {
					case <-ctx.Done():
						retryTimer.Stop()
						// 关闭瞬间重试中断：该条最终失败，补投死信（评审 #2）。
						wp.dlEnqueue(se, retried)
						if wp.stageMonitor != nil {
							wp.stageMonitor.record(time.Since(start), true)
							wp.stageMonitor.recordCode(se.Code)
						}
						return
					case <-retryTimer.C:
					}
					// 每次重试独立超时上下文。
					attemptCtx := ctx
					var attemptCancel context.CancelFunc
					if wp.timeout > 0 {
						attemptCtx, attemptCancel = context.WithTimeout(ctx, wp.timeout)
					}
					out, err = wp.process(attemptCtx, in)
					if attemptCancel != nil {
						attemptCancel()
					}
					if err == nil {
						retryTimer.Stop()
						goto success
					}
					retried++
					if wp.errPol.OnError != nil {
						wp.errPol.OnError(wrapStageError(wp.stageName, in, err, time.Since(start)), wp.stageName, in)
					}
				}
				// 重试耗尽：调用降级函数。
				if wp.errPol.Fallback != nil {
					var fbOut any
					fbOut, fbErr := wp.errPol.Fallback(procCtx, in)
					if fbErr == nil {
						if typedOut, ok := fbOut.(Out); ok {
							out = typedOut
							err = nil
							goto success
						}
						// 类型不匹配：记录错误，继续失败路径。
						err = fmt.Errorf("fallback 类型不匹配: %T", fbOut)
						if wp.errPol.OnError != nil {
							wp.errPol.OnError(wrapStageError(wp.stageName, in, err, time.Since(start)), wp.stageName, in)
						}
					} else {
						err = fbErr
						if wp.errPol.OnError != nil {
							wp.errPol.OnError(wrapStageError(wp.stageName, in, fbErr, time.Since(start)), wp.stageName, in)
						}
					}
				}
				wp.dlEnqueue(se, retried)
				if wp.stageMonitor != nil {
					wp.stageMonitor.record(time.Since(start), true)
					wp.stageMonitor.recordCode(se.Code)
				}
				return // 最终失败，该条数据不进入下游
			case ErrModeCollect:
				wp.dlEnqueue(se, 0)
				if wp.stageMonitor != nil {
					wp.stageMonitor.record(time.Since(start), true)
					wp.stageMonitor.recordCode(se.Code)
				}
				// 仅记录（已通过 onError/OnError 回调），继续处理下一条
				return
			}
		}
		// 未配置错误策略：默认 Collect 行为，失败数据进死信。
		wp.dlEnqueue(se, 0)
		if wp.stageMonitor != nil {
			wp.stageMonitor.record(time.Since(start), true)
			wp.stageMonitor.recordCode(se.Code)
		}
		return // 处理失败，该条数据不进入下游
	}

success:
	// 记录成功到监控统计（D-02）：含重试/降级消耗的总耗时。
	elapsed = time.Since(start)
	if wp.stageMonitor != nil {
		wp.stageMonitor.record(elapsed, false)
	}
	// 叶子 Stage（配置 sink）直接消费结果，不产生无人读取的 output（D-21）。
	if wp.sink != nil {
		wp.sink(procCtx, out)
		return
	}

	// 写 output 并测量阻塞时间（D-27，背压检测：output 满时 select 等待时长即为阻塞耗时）。
	writeStart := time.Now()
	select {
	case wp.out <- out:
		if wp.stageMonitor != nil {
			wp.stageMonitor.recordBlocked(time.Since(writeStart))
		}
	case <-ctx.Done():
		// 前驱已关闭：放弃写出，退出防死锁
	}
}

// wait 等待所有 worker 退出（Input 关闭 / ctx 取消后排空完成）。
func (wp *workerPool[In, Out]) wait() {
	wp.wg.Wait()
}

// retryDelay 计算第 n 次重试的等待间隔（D-31）：
// = RetryDelay × RetryBackoff^n；RetryBackoff ≤ 1 时固定为 RetryDelay。
// RetryBackoff ≤ 0 时按 1 处理（兼容旧行为：固定间隔）。
// 防止指数增长溢出：上限 1 小时。
func (wp *workerPool[In, Out]) retryDelay(n int) time.Duration {
	const maxDelay = time.Hour
	if wp.errPol == nil || wp.errPol.RetryBackoff <= 1 {
		if wp.errPol == nil || wp.errPol.RetryDelay <= 0 {
			return time.Millisecond
		}
		return wp.errPol.RetryDelay
	}
	base := float64(wp.errPol.RetryDelay)
	delay := base
	for i := 0; i < n; i++ {
		delay *= wp.errPol.RetryBackoff
		if delay > float64(maxDelay) {
			return maxDelay
		}
	}
	return time.Duration(delay)
}

// dlEnqueue 将一条最终失败数据写入死信队列（D-23）。
// dlWriter 为 nil（未启用死信）时直接返回，零开销。
// 写失败仅打印，不影响主流程（死信是尽力而为的补充通道）。
func (wp *workerPool[In, Out]) dlEnqueue(se *StageError, retried int) {
	if wp.dlWriter == nil || wp.dlWriter.sink == nil {
		return
	}
	rec := DeadLetterRecord{
		TS:      time.Now(),
		Stage:   se.Stage,
		Code:    se.Code,
		ErrMsg:  se.Error(),
		Latency: se.Latency,
		Retried: retried,
	}
	// 输入序列化：用户自定义 marshal 优先；失败降级为 fmt 文本。
	if wp.dlWriter.marshal != nil {
		if b, err := wp.dlWriter.marshal(se.Input); err == nil {
			rec.Input = b
		} else {
			rec.Input = []byte(fmt.Sprintf("%+v", se.Input))
		}
	} else if b, err := json.Marshal(se.Input); err == nil {
		rec.Input = b
	} else {
		rec.Input = []byte(fmt.Sprintf("%+v", se.Input))
	}
	if err := wp.dlWriter.sink.Write(rec); err != nil {
		// 死信写入失败不影响主流程，仅告警。
		if wp.stageName != "" {
			fmt.Printf("[deadletter] stage=%s write failed: %v\n", wp.stageName, err)
		} else {
			fmt.Printf("[deadletter] write failed: %v\n", err)
		}
	}
}
