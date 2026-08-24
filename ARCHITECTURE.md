# Pipeline 业务框架架构设计

> **变更约定**：本文件是 pipeline 项目的唯一权威设计文档。后续无论是新增需求还是修改需求，**必须先同步更新本文档并确认，再编写代码**。代码实现必须与本文档保持一致。

- 语言：Go 1.27
- 模块：`github.com/YUJIAJING0408/pipeline`
- 版本：v1.0.0（已发布）
- 更新日期：2026-08-22

---

## 1. 项目背景与目标

设计一个通用的 Go Pipeline 业务框架，支持将复杂业务拆解为多个 Stage 串联、树形 Fork 或汇聚合并执行。当前版本已实现**串行链 + 树形多分支（Fork）**拓扑、**条件路由（D-25）**与 **Keyed Join 汇聚（D-26）**。

### 核心目标

| 目标 | 说明 |
|------|------|
| 通用性 | 通过泛型支持任意业务数据类型，Stage 可复用、可插拔 |
| 数据流通 | Stage 之间通过 Channel 通信，缓冲区大小由用户配置 |
| 可监控 | 每个 Stage 独立记录耗时，支持超时取消与链路时间分析 |
| 优雅关闭 | Pipeline 关闭时逐层级联关闭，不丢失剩余数据 |
| 便捷使用 | 持续运行的 Pipeline，预留输入接口并内置 Mock 数据源 |
| 组件库形态 | 本库作为 Library 供其他项目 `import` 使用，不包含 `package main`（示例除外） |

---

## 2. 设计决策记录（ADR）

以下为本项目历次需求讨论的结论，后续变更需同步修改本文档。

| 决策点 | 结论 | 说明 |
|--------|------|------|
| D-01 数据流模型 | 泛型 Stage + Channel | Pipeline 管理根 Stage 的流向，Stage 间通过 Channel 流通数据（NextStage 连接父子、Fan-out 广播到各分支） |
| D-02 时间监控 | 强制内置 | 每个 Stage 运行中记录 total/errors/latency（`StageMonitor`），用户可调用 `Monitor.GenerateSummary()` 获取汇总或 `Monitor.Format()` 生成格式化报告（含各 Stage 的平均/最大耗时） |
| D-03 拓扑范围 | 串行 + 树形 Fork + 汇聚（已实现） | 通过 `NextStage` 多次调用可形成树形多分支拓扑（每个子 Stage 获得独立 input Channel，Fan-out 广播）；`routeFn` 支持条件路由（D-25）；`MergeNode` 支持 Keyed Fan-in 汇聚（D-26，见 8.6） |
| D-04 错误处理 | 三种模式全支持（已实现） | Fail-Fast（取消 Stage ctx 整链传播）、Collect（记录错误继续处理，汇总报告中呈现）、Retry+Fallback（按 MaxRetry/RetryDelay 重试，耗尽后降级函数）。`OnError` 回调所有模式触发。process 错误自动包装为 `StageError`（分类 + 上下文），支持 `errors.Is/As` 判型。策略通过 `Pipeline.ErrorPolicy(ErrPolicy)` 全局注入（经 params 传递给每个 Stage 的 workerPool），**各 Stage 也可通过 `StageConfig.ErrPolicy` 单独配置覆盖全局** |
| D-05 并发模型 | Stage 内 WorkerPool | 每个 Stage 内部维护自己的 WorkerPool，消费输入 Channel |
| D-06 运行形态 | 持续运行 + 自定义 InputSource | 框架不内置调度器，但支持自定义 InputSource 实现定时/事件驱动数据源（如 real 示例的 timedSource）；同时内置 MockSource 用于测试 |
| D-07 关闭机制 | 级联优雅关闭（递归） | `Stage.Close` 递归：先关闭自身（排空 WorkerPool、关闭 output、取消 forwardCtx），再递归关闭所有 subStages |
| D-08 日志 | 每 Stage 独立日志文件（JSON 结构化） | 每个 Stage 在 `Start` 时创建 `{stageName}.log` 独立日志文件（通过 `PipelineConfig.LogDir` 与 `LogEnabled` 控制），`Close` 时刷盘关闭。`LogEnabled` 为 false 时不创建文件。每行一条 JSON：`ts/stage/level/msg/fields`；四级日志级别（`PipelineConfig.LogLevel` 静态配置 / `SetLevel` 动态调整） |
| D-09 实现语言特性 | Go 泛型（Go 1.18+） | 项目使用 Go 1.27，泛型已成熟，支持类型安全的 Stage 类型 |
| D-10 交付形态 | 组件库（Library） | 供其他 Go 项目 import 调用，不提供可执行程序；示例代码仅存在于 examples/ 下 |
| D-11 Stage 类型形态 | 泛型 struct + 函数字段 | **替代接口方案**：`Stage` 为泛型 struct，处理函数作为字段注入；消除接口派发开销（业务场景 <0.1%，可忽略），API 从方法型转为函数型 |
| D-12 当前实现范围 | 核心链路已实现 | Pipeline / Stage / NextStage 链式 / subStages 递归 / 广播 Fan-out / 条件路由（D-25）/ Keyed 汇聚（D-26）/ Sink 消费 / 慢处理计时 / 泛型 WorkerPool / JSON 结构化日志 / 链路时间监控+P50/P99 / 三模式错误策略 / 错误类型体系 / 死信队列 / 生命周期钩子 / 实时指标面板 完整实现 |
| D-13 Stage 内嵌 Channel | `Stage[T1, T2]` 持私有 `input` / `output` 字段 | 输入 Channel 元素类型 T1、输出 Channel 元素类型 T2，双类型参数解耦；**字段私有不可导出**，外部经 `InChan() <-chan T1` / `OutChan() chan<- T2` 方向化访问 |
| D-14 Channel 生命周期 | NewStage 注入只读 inputChan + StageConfig.OutCap | `NewStage(name, cfg, inputChan <-chan T1, fn)`：**输入 Channel 由外部传入（只读视图）**；**输出 Channel 由 NewStage 内部创建，容量取 `StageConfig.OutCap`**。D-20 后每个子 Stage 通过 `NextStage` 获得独立 input Channel（存于 `subOutChans`），不再直接复用父 output |
| D-15 Stage.Start 与 Context 链 | `Stage.Start(ctx, params)` 递归启动 + forwardCtx 链 | `Stage.Start` 先递归启动所有 subStages（传入 forwardCtx），再启动自身 WorkerPool。Context 链由 `WithCancel(ctx)` 派生 forwardCtx 逐级传递 |
| D-16 Stage.Close 优雅退出 | `Stage.Close(drainTimeout)` 递归关闭 | 先关闭自身（排空 WorkerPool、关闭 output、取消 forwardCtx），再递归关闭所有 subStages。Pipeline.Close 只需调用根 Stage.Close |
| D-17 Pipeline.Run / Close | 递归编排整个链路 | `Pipeline.Run(ctx)`：① 校验未启动 → ② 根 Stage.Start（递归启动 whole 链）→ ③ 启动所有 InputSource 向首节点注入数据 → ④ 阻塞等待 ctx.Done()。`Pipeline.Close(timeout)`：调用根 Stage.Close（递归），返回首个错误。幂等 |
| D-18 Pipeline 配置 | `PipelineConfig` + `New[T1, T2](cfg)` | `New[T1, T2](cfg PipelineConfig)` 取代裸构造：`InputBufferSize`（首节点 inchan 缓冲区大小）、`Name`（Pipeline 名称）、`LogDir`（日志存储位置，默认 ./logs）、`LogEnabled`（整体日志开关）；`LogDir()` Builder 保留为便捷覆盖 |
| ~D-19~ 异构链实验 | ~~Type-State：`Pipeline[In, Out]`~~ | **已回滚**：曾尝试双类型参数 `Pipeline[In, Out]` + 内部 `stageNode` 装箱，因引入装箱复杂度与 GoLand 方法级泛型推断误报而**回滚**。后续 D-20 以 `Pipeline[T1, T2]` 形式采纳了双类型参数思想（T1=输入类型，T2=根 Stage 输入类型） |
| D-20 NextStage 链式设计 | `Stage.NextStage` + `subStages` 递归 | 每个 Stage 通过 `NextStage[T3]` 创建子 Stage（类型由编译器强制匹配：`T2` 父输出 = `T3` 子输入），`subStages []Stager` 持有子 Stage 列表。**每个子 Stage 获得独立的 input Channel**（存于父的 `subOutChans`）：父节点产出数据经**广播转发协程**复制分发给每个子 Stage（Fan-out）。`Start` 递归先启动子 Stage 再启动自身；`Close` 递归先关闭自身（含 `subOutChans`）再关闭子 Stage。`Pipeline[T1, T2]` 持有根 Stage（`T1`=输入源类型，`T2`=根 Stage 输入类型），`AddStage` 设置根 Stage |
| D-21 Fork 广播语义 | 子 Stage 独立通道 + 固定转发 worker + Sink 消费 | 早期 `NextStage` 直接复用 `s.output` 作为所有子 Stage 的 input（**竞争消费**：每条数据只被一个分支取走，不符合真实业务）。D-21 修复分三步：① 每个子 Stage 通过 `NextStage` 创建时获得**独立 outChan**（append 到 `subOutChans []chan T2`）；② **广播实现为「分发器 + 每分支固定转发 worker」**：`startFanout` 部署 1 个分发器 goroutine（读 `s.output`）+ 每个子分支 1 个常驻转发 worker（通过有界队列 `fanoutQueues` 接收数据，写入对应 `subOutChan`，超时 `StageConfig.Timeout`）。**goroutine 总数恒定 = 1 + 分支数，与数据量无关**；③ **Sink 语义**：叶子 Stage 通过 `Sink(fn)` 将 process 结果直接交给消费函数（统计/落库），不再写入无人消费的 output channel——避免树形叶子背压死锁（叶子无消费者 → output 满 → 整链冻结）。`Close` 顺序：`close(output)` → `close(fanoutQueues)` → `Wait` → `close(subOutChans)`。语义：**每条数据被复制分发给每个子 Stage，各分支独立处理**（Fan-out） |
| D-22 慢处理计时 | `StageConfig.SlowThreshold` + workerPool 计时 | 每个 Stage 的 `StageConfig` 增加 `SlowThreshold time.Duration`（慢处理阈值，0 表示不启用）。workerPool 对每条 process 计时，耗时**超过阈值即打印慢日志**（`[slow] stage=%s input=%v took=%v threshold=%v`），不区分成功/失败（失败同样提示，便于诊断） |
| D-23 死信队列 | 已实现（8.5） | 处理失败数据投递到 `DeadLetterSink` 接口（默认 JSONL 落盘 `{Dir}/{stage}.dl.jsonl`）；自定义 sink 无缝接入。`DrainDeadLetters` 读取 + `DeadLetterReplaySource` 重放 |
| D-24 生命周期钩子 | 已实现 | `StageHooks` 嵌入 process 前后：`OnBeforeProcess`（注入 ctx）/ `OnAfterProcess`（捕获 out/err/latency），零侵入实现 trace / 审计 / 限流等横切关注点 |
| D-25 条件路由 | 已实现 | `NewStage` / `NextStage` 的 `routeFn func(T1) bool` 参数（nil=放行）。父节点投递前检查子节点的 `routeFn`，false 则跳过该分支。零运行时开销：`fanoutRouteFuncs` 平行切片，编译期类型安全，无接口断言 |
| D-26 条件汇聚（Keyed Fan-in） | 已实现（8.6） | `MergeNode` 按 `MergeKey` 凑齐 N 个分支结果后合并；单收集 goroutine 零锁 + 过期扫描清理 + **引用计数关闭**；`Attach` 注册生命周期子节点（不参与数据转发）；`NextStage` 支持多次调用创建多个下游分支，**routeFn** 支持条件路由（D-25） |
| D-27 背压可视化 | 已实现（7.4） | `StageMonitor` 新增 `blockedTime` + `depthFn`，`Metrics()` 输出 `QueueDepth`/`BlockedTime`；MetricsServer SSE 帧含 `queueDepth`/`blockedTimeNs`；前端面板显示"积压/阻塞/条/阻塞总"四列 |
| D-28 拓扑校验 | 已实现（9.1） | `Pipeline.Validate()` 构建期校验：数据流无环（DFS 三色）/ MergeNode 分支数匹配 / 生命周期已挂载 / 分支与下游不重复 / 无孤立节点；一次性返回全部错误 |
| D-29 限流器 | 已实现（7.5） | 内置令牌桶 `RateLimiter`（Allow 丢弃式 / Wait 背压式），`StageConfig.RateLimiter` 按 Stage 注入，process 前限流，保护下游慢依赖 |
| D-30 MergeNode 可观测性 | 已实现（8.6） | `JoinConfig` 增加 `SlowThreshold`（merge 慢日志，D-22 对齐）/ `RateLimiter`（merge 前限流，D-29 对齐）/ `OnMergeError`（merge 失败回调，D-04 对齐）；MergeNode 接入 `stageMonitor`（Metrics/MetricsServer 可观测 merge 环节） |
| D-31 重试退避 | 已实现（8.3） | `ErrPolicy.RetryBackoff`：第 n 次重试间隔 = `RetryDelay × Backoff^(n-1)`；0 或 1 = 固定间隔（兼容旧行为），>1 = 指数退避，降低对下游瞬时故障的冲击 |
| D-32 默认路由分支 | 已实现（8.6） | `Stage.SetDefaultBranch(st)` 将某分支标记为默认兜底：当其他分支的 routeFn 全部不匹配时，数据投递给默认分支（不静默丢弃） |

### 已暂时排除的内容（YAGNI）

- 持久化、定时调度、事件驱动（D-06）
- 动态热加载、配置驱动（后续有需要再评估）
- 优先级队列、限流（后续有需要再评估）

---

## 3. 总体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Pipeline[T1,T2]  编排层                                                     │
│  T1=输入源类型，T2=根 Stage 输入类型（AddStage 设置根 Stage）                  │
│                                                                             │
│  root Stage ── 广播转发(Fan-out, D-21) ──► subStage1 ──► (子分支可再分叉)      │
│   │  ┌────────┐       ┌────────┐      ┌────────┐                            │
│   │  │Worker  │       │Worker  │      │Worker  │                            │
│   │  │Pool    │       │Pool    │      │Pool    │                            │
│   │  └────────┘       └────────┘      └────────┘                            │
│   │  subOutChans:     每个子 Stage 独立 input Channel                        │
│   │  每条数据按 routeFn 过滤（D-25）后分发到匹配分支（固定转发 worker）          │
│   │  ┌──────────────────────────────────────────────────────┐               │
│   │  │  MergeNode（D-26）：按 MergeKey 凑齐 N 分支 → 合并 → 下游│               │
│   │  │  Attach 挂到各分支（仅生命周期/拓扑，不参与数据转发）     │               │
│   │  └──────────────────────────────────────────────────────┘               │
│   └── subStages ──► 递归 Start/Close：先子后父 / 先父后子                     │
│        ▲  关闭信号：forwardCtx 链 + subStages 递归（D-16）                     │
│                                                                             │
│   InputSource（注入首节点）   Monitor（汇总链路时间分析）                      │
│   ErrPolicy（错误策略全局注入）  MetricsServer（实时指标面板）                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 组件职责

| 组件 | 职责 | 归属 |
|------|------|------|
| `Stage[T1, T2]` | 单节点业务处理单元（泛型 struct + 函数字段 + 私有 Channel + Context 链，D-11/D-13/D-15） | 框架提供 + 用户注入函数 |
| `Stage.NextStage[T3]` | 创建子 Stage，为其创建**独立 input Channel** 并 append 到父的 `subOutChans`（D-21） | 框架提供 |
| `subStages` | 每个 Stage 持有的子 Stage 列表（`[]Stager`，递归 Start/Close 用） | 框架提供（不导出） |
| `subOutChans` | 每个子 Stage 独立的数据通道（`[]chan T2`，广播转发目标，D-21） | 框架提供（不导出） |
| 广播转发 | `startFanout`：1 个 dispatcher + 每分支固定转发 worker（有界队列，带超时，goroutine 恒定） | 框架提供 |
| `Stage.Sink` | 将叶子 Stage 的 process 结果交给消费函数（统计/落库），不再写入无人消费的 output（D-21 防背压死锁） | 框架提供 |
| `Stage.Attach` | 将外部节点（如 MergeNode）注册为本 Stage 的生命周期子节点，不参与数据转发（D-26） | 框架提供 |
| `StageConfig.RouteFn` | 条件路由：父节点投递前检查子分支 routeFn，false 则跳过（D-25，nil=放行） | 框架提供 |
| `StageConfig.SlowThreshold` | 慢处理阈值：单条耗时超过即打印慢日志（D-22），0 表示不启用 | 框架提供 |
| `workerPool` | Stage 内部泛型工作池（监听 input → Process → 写 output / sink / 慢日志，D-15/D-22） | 框架提供（不导出） |
| `Pipeline[T1, T2]` | 编排层，持有根 Stage + 管理输入源注入 | 框架提供 |
| `InputSource[In]` | 向首节点注入数据；提供 Mock 实现 | 框架 + 用户 |
| `ErrPolicy` | 错误处理策略统一配置（Fail-Fast / Collect / Retry+Fallback，D-04 已实现） | 用户配置 |
| `StageLogger` | 每 Stage 独立 JSON 结构化日志文件（`{stageName}.log`，四级级别 + fields，D-08） | 框架提供 |
| `Monitor` | 聚合各 Stage 统计，`GenerateSummary()` 返回汇总、`Format()` 生成格式化报告（D-02） | 框架提供 |
| `MetricsServer` | 实时指标面板：标准库 HTTP + SSE 推送吞吐/P50/P99（7.4） | 框架提供 |
| `DeadLetterSink` / `JSONLDeadLetterSink` | 死信落盘抽象 + 默认 JSONL 实现（D-23） | 框架提供 |
| `MergeNode[T Keyable]` | 条件汇聚节点：按 MergeKey 凑齐 N 分支后合并（D-26） | 框架提供 |

---

## 4. 核心类型设计

### 4.1 泛型 Stage（D-11：struct + 函数字段）

```go
// StageConfig 描述单个 Stage 的运行时配置（D-14/D-22/D-24/D-04）。
type StageConfig struct {
    Workers       int           // WorkerPool 并发数，必须 ≥ 1
    OutCap        int           // 当前 Stage 输出 Channel 容量，必须 ≥ 0
    Timeout       time.Duration // 单条数据处理超时，0 表示不超时
    SlowThreshold time.Duration // 慢处理阈值：单条耗时超过即打印慢日志，0 表示不启用（D-22）
    Hooks         StageHooks    // process 生命周期钩子（D-24，前后回调，均为可选）
    ErrPolicy     *ErrPolicy    // 本 Stage 错误策略（D-04，非 nil 时覆盖全局策略）
}

// StageHooks 定义 process 执行前后的生命周期回调（D-24）。
type StageHooks struct {
    OnBeforeProcess func(ctx context.Context, in any) context.Context
    OnAfterProcess  func(ctx context.Context, in any, out any, err error, latency time.Duration)
}

// Stage 是单节点业务处理单元：泛型 struct，处理逻辑以函数字段注入（D-11）。
type Stage[T1, T2 any] struct {
    name    string                                        // Stage 名称
    config  StageConfig                                   // 运行时配置
    process func(ctx context.Context, in T1) (T2, error)  // 单条数据处理（用户注入）
    input   <-chan T1                                     // 输入 Channel（NewStage 注入，私有）
    output  chan T2                                       // 输出 Channel（NewStage 创建，私有）
    routeFunc func(T1) bool                               // 本 Stage 的路由条件（D-25，nil=放行）
    subStages []Stager                                    // 子 Stage 列表（递归 Start/Close）
    subOutChans []chan T2                                 // 每个子 Stage 独立输入 Channel（Fan-out）
    fanoutQueues []chan T2                                // 广播转发有界队列（固定 worker 消费）
    fanoutRouteFuncs []func(T2) bool                      // 每分支路由条件（D-25，与 fanoutQueues 一一对应）
    fanoutWG sync.WaitGroup                               // 转发 worker 等待组
    dispatcherWG sync.WaitGroup                           // 分发器等待组
    running atomic.Bool                                   // Start 校验用（D-15）
    mu sync.Mutex                                         // Start/Close 互斥
    ctx          context.Context                          // 生命周期 Context（WithCancel 派生，D-15）
    cancel       context.CancelFunc                       // 取消 ctx（Close 超时兜底）
    forwardCtx   context.Context                          // 派发给后继（D-15/D-16）
    forwardCancel context.CancelFunc                      // 关闭时通知后继
    pool *workerPool[T1, T2]                              // 内部工作池
    sink func(ctx context.Context, v T2)                  // 叶子消费函数（D-21）
    logger *StageLogger                                   // 独立日志文件（D-08）
    stageMonitor *StageMonitor                            // 耗时统计（D-02）
    errPol *ErrPolicy                                     // 错误策略（D-04）
    dlWriter *deadLetterWriter                            // 死信写入器（D-23，持有 sink + marshaler）
    dlOwnSink bool                                        // sink 是否由本 Stage 创建（D-23）
}

// Stager 是 Stage 运行时接口，供 subStages 列表统一管理。
type Stager interface {
    Start(ctx context.Context, params map[string]any) error
    Close(drainTimeout time.Duration) error
    Name() string
    Describe(parent string)
    GraphTD(w *graphTDWriter)
}

// NewStage 创建 Stage。参数：名称、配置、输入 Channel（只读视图）、路由条件（nil=放行）、处理函数。
func NewStage[T1, T2 any](name string, cfg StageConfig, inputChan <-chan T1, routeFn func(T1) bool, fn func(ctx context.Context, in T1) (T2, error)) *Stage[T1, T2]

// NextStage 创建子 Stage 并连线（子.input = 独立 Channel），返回子 Stage 引用。
// routeFn：子 Stage 的路由条件（nil=放行所有数据到该分支，D-25）。
func (s *Stage[T1, T2]) NextStage[T3 any](name string, cfg StageConfig, routeFn func(T2) bool, fn func(ctx context.Context, in T2) (T3, error)) *Stage[T2, T3]

// Sink 将 process 结果交给消费函数，不再写 output（叶子防背压死锁，D-21）。
func (s *Stage[T1, T2]) Sink(fn func(ctx context.Context, v T2)) *Stage[T1, T2]

// Attach 将 st 注册为本 Stage 的生命周期子节点：Start/Close/Describe/GraphTD 递归遍历，
// 但不参与数据转发（不创建 fanout 分支）。用于 MergeNode 这类消费本 Stage 输出的外部节点（D-26）。
func (s *Stage[T1, T2]) Attach(st Stager)

// InChan / OutChan / Name / Config / Process / ForwardCtx 见代码。
func (s *Stage[T1, T2]) InChan() <-chan T1
func (s *Stage[T1, T2]) OutChan() chan<- T2
func (s *Stage[T1, T2]) Name() string
func (s *Stage[T1, T2]) Config() StageConfig
func (s *Stage[T1, T2]) Process(ctx context.Context, in T1) (T2, error)
func (s *Stage[T1, T2]) ForwardCtx() context.Context

// Describe 递归打印父子关系。
func (s *Stage[T1, T2]) Describe(parent string)

// GraphTD 输出 Mermaid graph TD 格式流程图。
func (s *Stage[T1, T2]) GraphTD(w *graphTDWriter)

// Start 递归启动：先启动所有 subStages（含 Attach 节点），再启动自身 WorkerPool 与广播转发。
// ctx 为上游传入的 Context；params 承载 logDir/logEnabled/logLevel/monitor/errPol/deadLetter。
func (s *Stage[T1, T2]) Start(ctx context.Context, params map[string]any) error

// Close 递归关闭（两阶段）：先等自然排空，超时后取消 ctx 兜底，关闭 fanout 队列/subOutChans，
// 再关闭 Attach 节点与 subStages。
func (s *Stage[T1, T2]) Close(drainTimeout time.Duration) error
```

### 4.2 WorkerPool（Stage 内部，D-15：泛型实现 + D-22 慢日志 + D-04 错误策略）

```go
// workerPool 是 Stage 内部的数据处理工作池（泛型，位于根包 workerPool.go）。
type workerPool[In, Out any] struct {
    workers int
    in      <-chan In
    out     chan<- Out
    process func(ctx context.Context, in In) (Out, error)
    timeout time.Duration
    onError func(err error, in In)
    sink    func(ctx context.Context, v Out)       // 叶子消费（D-21）
    stageName string                                // 慢日志标识
    slowThreshold time.Duration                     // 慢处理阈值（D-22）
    stageMonitor *StageMonitor                      // 耗时统计（D-02）
    errPol *ErrPolicy                                // 错误策略（D-04）
    hooks *StageHooks                                // 生命周期钩子（D-24）
    dlWriter *deadLetterWriter                       // 死信写入器（D-23）
    cancel func()                                    // FailFast 取消函数
    wg sync.WaitGroup
}

// newWorkerPool 创建工作池。
func newWorkerPool[In, Out any](workers int, in <-chan In, out chan<- Out,
    process func(ctx context.Context, in In) (Out, error), timeout time.Duration,
    onError func(err error, in In)) *workerPool[In, Out]

// start 启动 N 个 worker 协程。
func (wp *workerPool[In, Out]) start(ctx context.Context)

// wait 等待所有 worker 退出。
func (wp *workerPool[In, Out]) wait()

// handle 处理单条数据：OnBeforeProcess → process（受超时约束，panic 被 recover 转为错误）
// → OnAfterProcess → 错误策略分支 → 成功写 output / sink。失败数据经 dlEnqueue 进死信（D-23/D-24）。
func (wp *workerPool[In, Out]) handle(ctx context.Context, in In)

// dlEnqueue 将最终失败数据写入死信队列（D-23）；dlWriter 为 nil 时零开销跳过。
func (wp *workerPool[In, Out]) dlEnqueue(se *StageError, retried int)
```

### 4.3 Pipeline 编排层

```go
// ErrMode 定义 Pipeline 的错误处理模式。
type ErrMode uint8

const (
    ErrModeFailFast      ErrMode = iota // 任一 Stage 失败 → 整条 Pipeline 停止
    ErrModeCollect                      // 记录错误，继续处理，结束时汇总
    ErrModeRetryFallback                // 重试 N 次，仍失败则走降级函数
)

// ErrPolicy 统一描述错误处理策略。
// 设计取舍：ErrPolicy 刻意保持**非泛型**，以便作为 Pipeline 级共享配置。
// 因此 Fallback / OnError 回调中的 in/out 使用 interface{}（别名 any），
// 由框架在调用点基于当前 Stage 的载荷类型 T 做安全类型断言。
type ErrPolicy struct {
    Mode        ErrMode
    MaxRetry    int           // RetryFallback 模式：最大重试次数
    RetryDelay  time.Duration // RetryFallback 模式：重试间隔
    Fallback    func(ctx context.Context, in any) (out any, err error) // 降级函数
    OnError     func(err error, stageName string, in any) // 错误回调（所有模式都会触发）
}

// PipelineConfig 描述 Pipeline 的整体配置（D-18）。
type PipelineConfig struct {
    Name            string // Pipeline 名称，用于日志/监控标识
    InputBufferSize int    // 首节点 inchan 缓冲区大小（0 表示无缓冲，天然背压）
    LogDir          string // 日志存储位置，空则默认 ./logs
    LogEnabled      bool   // 整体日志开关；false 时各 Stage 日志不落盘
    LogLevel        LogLevel // 各 Stage 日志级别（默认 LogLevelInfo，D-08）
    DeadLetter      *DeadLetterConfig // 死信队列配置；nil 表示不启用（D-23）
}

// Pipeline 编排多个 Stage 串联执行（D-01 / D-03）。
// T1 = 输入源数据类型，T2 = 根 Stage 输入类型。
// Stage 之间的连接通过 NextStage 完成（非 Pipeline 管理）。
type Pipeline[T1, T2 any] struct {
    config  PipelineConfig     // 整体配置（D-18）
    stage   *Stage[T1, T2]     // 根 Stage（AddStage 设置）
    sources []InputSource[T1]  // 输入源（当前支持多源同时注入）
    errPol  ErrPolicy
    monitor *Monitor
    output  chan T1            // 首节点输入通道（New 创建，缓冲 = InputBufferSize）
    started bool               // Run 是否已调用
    closed  bool               // Close 是否已完成
}

// New 创建带配置的空 Pipeline（D-18），随后通过 Builder 方法组装。
func New[T1, T2 any](cfg PipelineConfig) *Pipeline[T1, T2]

// AddStage 设置根 Stage，将其 input 连到 Pipeline 的 output 通道。
// Stage 链式连接通过 NextStage 完成（非 Pipeline 管理）。
func (p *Pipeline[T1, T2]) AddStage(stage *Stage[T1, T2]) *Pipeline[T1, T2]

// Input 注册输入源；框架内置 MockSource，用户也可实现 InputSource[T1]。
func (p *Pipeline[T1, T2]) Input(src InputSource[T1]) *Pipeline[T1, T2]

// ErrorPolicy 设置全局错误策略。
func (p *Pipeline[T1, T2]) ErrorPolicy(pol ErrPolicy) *Pipeline[T1, T2]

// LogDir 设置日志输出目录（默认 ./logs，每个 Stage 一个文件）。便捷覆盖 config.LogDir。
func (p *Pipeline[T1, T2]) LogDir(dir string) *Pipeline[T1, T2]

// MetricsMonitor 返回 Pipeline 内部的 Monitor，供 MetricsServer / 自定义巡检接入（7.4）。
func (p *Pipeline[T1, T2]) MetricsMonitor() *Monitor

// DrainDeadLetters 读取指定 Stage 的全部死信记录（D-23，默认 JSONL 落盘场景；读后不清除文件）。
func (p *Pipeline[T1, T2]) DrainDeadLetters(stage string) ([]DeadLetterRecord, error)

// Describe 打印链路上各 Stage 名称的摘要（示例与调试用）。
func (p *Pipeline[T1, T2]) Describe()

// GraphTD 返回 Mermaid graph TD 格式的流程图。
func (p *Pipeline[T1, T2]) GraphTD() string

// Run 启动 Pipeline（D-17）：调用根 Stage.Start（递归启动 whole 链）→
// 并发启动输入源注入首节点，全部返回后统一 close → 阻塞等待 ctx 取消 → 返回后由 Close 级联关闭。
func (p *Pipeline[T1, T2]) Run(ctx context.Context) error

// Close 触发级联优雅关闭（D-17）：调用根 Stage.Close（递归关闭 whole 链）。
// 幂等：未启动/已关闭返回 nil。
func (p *Pipeline[T1, T2]) Close(timeout time.Duration) error
```

### 4.4 输入源与 Mock

```go
// InputSource 提供 Pipeline 的初始输入。用户在 Pipeline 上调用 Input() 注册。
// 契约：Start 向 out 持续发送数据，数据全部发完或 ctx 取消时返回（**不负责 close**）；
// close 由 Pipeline.Run 等待所有输入源返回后统一执行。
type InputSource[In any] interface {
    // Start 启动注入逻辑，通过 out 发送数据；ctx 取消时停止。发送完毕自然返回。
    Start(ctx context.Context, out chan<- In) error
}

// MockSource 内置测试输入源：按顺序发送固定数据后自然返回。
type MockSource[In any] struct {
    Data []In
}
```

---

## 5. 数据流与运行模型

```
InputSource ──► output(T1) ──► root Stage(T1→T2) ──NextStage──► subStage(T2→T3) ──NextStage──► …
```

1. **构建（Build）**：`New[T1, T2](cfg).AddStage(s1)`，Stage 链通过 `s1.NextStage(s2).NextStage(s3)…` 连接。首节点 inchan 缓冲区由 `cfg.InputBufferSize` 决定（Pipeline.New 时创建 output 通道，AddStage 接管为根 Stage 的 input；0 表示无缓冲，天然背压）。
2. **启动（Run，从前往后，D-15）**：
   - `Stage.Start` 递归：先启动所有 subStages / Attach 节点（传入 forwardCtx），再启动自身 WorkerPool（监听 input）；
   - Context 链：`s1.Start(root)` → 内部 `s1` 以 `WithCancel(root)` 派生 forwardCtx 传给 subStage → `s2.Start(forwardCtx)` → …；
   - 启动 InputSource，向 Pipeline 的 output 通道写入数据（即根 Stage 的 input）；
   - params 经 Run 注入 monitor / errPol / 日志配置 / 死信配置，各 Stage 据此创建 stageMonitor 等。
3. **运转（Process）**：每个 Stage 从输入 Channel 取数据 → `Process` 执行（受 `Timeout` 约束，前后触发 Hooks D-24）→ 按 `ErrPolicy` 处理错误 → 输出写入下游 Channel（天然背压，缓冲区满则阻塞）。process 错误自动包装为 `StageError`（D-04）并投递死信（D-23）。
4. **关闭（Close）**：见 §6 级联关闭。

### Channel 缓冲区说明（D-14）

- 每个 Stage 的**输出 Channel** 由 NewStage 内部创建，缓冲区 = `StageConfig.OutCap`（每个 Stage 的配置保存自身输出 Channel 容量）；
- 每个 Stage 的**输入 Channel** 由外部传入（NewStage 的 inputChan 参数，只读视图），首节点通常来自 Pipeline 的 output 通道（New 时创建）；
- NextStage 连线：子 Stage 获得**独立 input Channel**（父的 `subOutChans[i]`），父产出经 fanout 转发复制分发（D-21）；条件路由按 `fanoutRouteFuncs[i]` 过滤（D-25）；
- 这是流水线的**背压机制**：下游处理慢时，上游写入会被阻塞，起到限流保护作用；
- 缓冲区满时 `Pipeline` 的链路整体慢下来，由监控数据反映瓶颈节点。

---

## 6. 优雅关闭（级联关闭机制，D-16/D-20）

关闭机制基于 **递归 Close**：每个 Stage 的 Close 先关闭自身（排空 WorkerPool、关闭 output、
关闭 fanout 队列/subOutChans、取消 forwardCtx），再递归关闭所有 Attach 节点与 subStages。
Pipeline.Close 只需调用根 Stage 的 Close，整条链即从前往后逐级关闭。

```
Pipeline.Close(timeout)  →  根 Stage.Close(timeout)
   │
   ▼
┌────────────────────────────────────────────────┐
│ 根 Stage.Close()                                │
│  (a) 等待自身 WorkerPool 排空在途数据（drainTimeout 内）│
│  (b) 关闭自身 output                            │
│  (c) 等待 fanout dispatcher 读完 output 剩余数据，  │
│      关闭各 fanoutQueues → 等待转发 worker 排空，   │
│      最后关闭 subOutChans（D-21）                 │
│  (d) 取消 forwardCtx（通知后继 Stage）            │
│  (e) 关闭死信 sink（若自建）→ 关闭日志              │
│  (f) 递归关闭所有 subStages（按添加顺序）           │
│      ├─ subStage1.Close()  → 递归自身 subStages  │
│      └─ subStage2.Close()  → …                  │
└────────────────────────────────────────────────┘
   │
   ▼
所有 Stage 依次关闭完成 → 整条链路关闭完成
│         所有 StageLogger 日志刷盘
│         Pipeline 返回
```

### 关闭关键点

1. **递归 Close（D-20）**：`Stage.Close` 先完成自身排空 + 关闭 output + 关闭 fanout 队列/subOutChans + 取消 forwardCtx，再递归关闭 `subStages`（含 `Attach` 节点）。Pipeline.Close 只需调用根 Stage 的 Close。
2. **双重关闭信号**：数据级（关闭 output Channel → fanout 队列 → subOutChans）+ 控制级（取消 forwardCtx）。
3. **排空优先**：每个 Stage 收到输入关闭/取消后，先处理完在途数据，再停止自己。
4. **超时兜底**：任一 Stage Close 排空超时（`drainTimeout`），返回 `ErrCloseTimeout` 并强制取消 ctx 停止，不留悬挂协程。
5. **输出 Channel 关闭职责**：输出 Channel 由所属 Stage 的 Close 负责关闭，Pipeline 不直接关闭，避免重复 close 引发 panic。
6. **幂等**：Close 可重复调用，running 已复位时第二次调用直接返回成功。
7. **指标报告**：`Monitor.GenerateSummary()` / `Format()` 为按需调用 API（不再由 Close 自动打印），用户显式获取链路时间分析。

---

## 7. 日志与监控设计

### 7.1 每 Stage 独立日志（D-08，JSON 结构化）

- 日志目录：默认 `./logs/`（可通过 `LogDir` 修改）；
- 文件命名：`{stageName}.log`，Stage 与日志文件一一对应；
- 每个 Stage 内部持有独立的 `*StageLogger`，互不干扰；
- 输出 **JSON 结构化日志**，每行一条，字段固定：`ts`（RFC3339Nano）/ `stage` / `level` / `msg` / `fields`（可选）；
- **日志级别**：`LogLevelDebug` < `LogLevelInfo`（默认）< `LogLevelWarn` < `LogLevelError`，低于配置级别的记录直接丢弃；
- 级别控制：`PipelineConfig.LogLevel` 静态配置，`StageLogger.SetLevel()` 运行期动态调整；
- 写日志 API：`Debugf/Infof/Warnf/Errorf`（格式化）+ `Debugw/Infow/Warnw/Errorw`（结构化字段 + `F(key, value)`）；`Printf` 保留兼容（等价 Info）；
- 并发安全：写文件由内部 mutex 串行化；字段含无法序列化值（chan/func）时降级为仅 msg 行。

```
logs/
├── stage-source.log        ← Stage 1 日志
├── stage-transform.log     ← Stage 2 日志
├── stage-filter.log        ← Stage 3 日志
└── stage-x.dl.jsonl        ← 死信队列文件（D-23，启用死信时按 Stage 生成）
```

> 注：链路汇总报告（`Monitor.Format()`）为按需调用，不再自动落盘。

### 7.2 日志内容示例

```
{"ts":"2026-08-22T20:47:22.608638+08:00","stage":"validate","level":"info","msg":"stage started","fields":{"stage":"validate"}}
{"ts":"2026-08-22T20:47:22.658808+08:00","stage":"validate","level":"info","msg":"stage closed","fields":{"stage":"validate"}}
{"ts":"2026-08-22T20:47:23.001000+08:00","stage":"parse","level":"error","msg":"item processing failed","fields":{"stage":"parse","input":3,"error":"reject 3"}}
```

可直接被 `jq` / ELK / Loki 等日志管道采集分析。

### 7.3 时间监控（D-02，已实现）

`StageMonitor` 每个 Stage 一个实例，由 `workerPool.handle` 在每次 process 后记录耗时与成败，
并将其写入 4096 条环形缓冲（滑动窗口，覆盖最旧样本），用于计算分位数：

| 指标 | 说明 |
|------|------|
| `total` | 已处理消息总计 |
| `errors` | 处理失败消息数 |
| `totalLatency` | 累计处理耗时（用于计算平均） |
| `avgLatency` | 平均单条耗时 |
| `maxLatency` | 单条最大耗时（瓶颈定位） |
| `lastLatency` | 最近一次耗时 |
| `P50` / `P99` | 最近 4096 条样本的分位数耗时（线性插值法，窗口内即时变化） |

### 7.4 实时指标面板（已实现）

`MetricsServer` 基于标准库 `net/http` 提供实时 Web 面板，零第三方依赖：

- `GET /` → `embed` 内嵌 `index.html` 前端页面（纯原生 JS，无外部 CDN）；
- `GET /metrics` → **SSE 流**，按 `RefreshInterval`（默认 1s）推送一帧全量 JSON 快照；
- `snapshot` 计算各 Stage 相对上一帧的**吞吐量**（条/秒），帧内含：
  `total / errors / throughput / avgLatency / maxLatency / p50 / p99 / queueDepth / blockedTime`；
- `Monitor.Metrics()` 为纯查询 API：不持有全局锁计算分位（仅逐个锁 StageMonitor），供
  外部巡检 / Prometheus 抓取等场景复用；
- **背压指标**（D-27）：`queueDepth` 为当前输入队列积压条数（`len(input)`），
  `blockedTime` 为累计 output 写阻塞耗时（每条数据写下游时若 output 满则计等待时间）；
  前端面板显示"积压/阻塞/条/阻塞总"四列，一目了然瓶颈位置。

**挂载方式**——`MetricsServer` 提供三种 handler 接入方式：

```go
// 方式一：独立端口（默认，使用 Start/StartAsync）
ms := &pipeline.MetricsServer{Monitor: pl.MetricsMonitor(), Addr: ":8080"}
go ms.Start()

// 方式二：挂载到用户自己的 HTTP 服务（完整路由）
mux := http.NewServeMux()
mux.Handle("/pipeline/", http.StripPrefix("/pipeline", ms.Handler()))

// 方式三：只挂载 SSE 流或主页
mux.Handle("/metrics", ms.MetricsHandler())
mux.Handle("/", ms.IndexHandler())
```

应用场景：

- **超时取消**：单条 `Process` 超过 `StageConfig.Timeout` 时，通过 `context.WithTimeout` 取消该任务；
- **链路时间分析**：用户可随时调用 `Monitor.GenerateSummary()` 获取所有 Stage 统计汇总（含各 Stage 平均/最大耗时），或 `Monitor.Format()` 生成格式化报告字符串，由调用方决定输出方式，可定位整条链路的时间瓶颈；
- **实时监控**：打开 `http://host:port/` 即可看到各 Stage 实时吞吐与延迟分位（SSE 自动刷新）；
- **背压定位**：观察"积压"列（队列深度）和"阻塞/条"列（平均写等待耗时），快速定位瓶颈 Stage。

### 7.5 限流器（D-29）

内置令牌桶限流器，保护下游慢依赖（第三方 API / 数据库连接池等有 QPS 上限的外部系统）：

```go
// 令牌桶：每秒补充 rate 个令牌，桶容量 burst（允许突发）
limiter := pipeline.NewRateLimiter(100, 20) // 100/s，突发 20

s := pipeline.NewStage("call-api", pipeline.StageConfig{
    RateLimiter: limiter, // nil = 不限流（零开销）
    Workers: 4, OutCap: 16,
}, nil, nil, fn)

// 两种限流语义
func (rl *RateLimiter) Allow(ctx context.Context) bool // 丢弃式：令牌不足返回 false，数据跳过（进死信）
func (rl *RateLimiter) Wait(ctx context.Context) error // 背压式：阻塞等待令牌，保持顺序（不可丢消息）
```

**实现**：`handle` 在 process 前调用限流器；`Wait` 支持 ctx 取消（限流等待超时与整体取消一致）。
`Allow` 语义下被拒数据走正常错误路径（onError / 死信），不静默丢弃。

---

## 8. 错误处理策略

三种模式（D-04）由 `pipeline.ErrorPolicy(ErrPolicy)` 全局注入每个 Stage：

### 8.1 Fail-Fast（快速失败）

- 任一 Stage 单条处理失败 → 调用 `cancel`（取消本 Stage 的 forwardCtx）→ 整链传播停止；
  失败数据在触发前已投递死信队列（D-23）；
- 适用：强一致、不允许中间状态丢数据的场景。

### 8.2 Collect（容错收集）

- 处理失败的消息记录到错误日志并计数，Pipeline 继续处理后续消息；
- `Close` 完成后在汇总报告中呈现错误数量；
- 适用：批处理、告警类可容忍部分失败的场景。

### 8.3 Retry + Fallback（重试降级）

- 同一条消息先按 `MaxRetry` / `RetryDelay` 重试；
- **指数退避（D-31）**：第 n 次重试间隔 = `RetryDelay × RetryBackoff^(n-1)`（Backoff=2 时 100ms → 200ms → 400ms），
  Backoff ≤ 1 时退化为固定间隔（兼容旧行为）；
- 重试耗尽仍失败 → 调用 `Fallback` 降级函数：
  - 降级成功 → 其输出继续走链路；
  - 降级失败 → 根据 `Mode`（Fail-Fast 或 Collect 行为）处理。
- 适用：依赖外部系统、存在瞬时故障的场景。

> 所有模式下，错误都会触发 `OnError` 回调（若配置），便于用户接入外部监控。

### 8.4 错误类型体系（已实现）

workerPool 自动将 process 返回的错误包装为 `*StageError`，携带处理上下文与错误分类，
支持 `errors.Is / errors.As` 程序化判型：

| API | 说明 |
|-----|------|
| `StageError.Stage` | 出错 Stage 名称 |
| `StageError.Input` | 触发错误的输入值（可能敏感，谨慎落盘） |
| `StageError.Latency` | 单条处理耗时 |
| `StageError.Code` | 错误分类（见下表） |
| `WithCode(err, code)` | process 内部显式标注分类 |
| `AsStageError(err)` | 从错误链提取 `*StageError` |
| `errors.Is(err, ErrTimeout)` 等 | 哨兵匹配，等价于 `Code` 命中 |

| Code | 触发场景 | 重试价值 |
|------|---------|---------|
| `CodeTimeout` | `context.DeadlineExceeded` / 单条超时 | 高（可重试） |
| `CodeInvalidInput` | 输入校验失败 | 无（直接丢弃） |
| `CodeProcessing` | process 返回普通错误（默认） | 视业务 |
| `CodeSystem` | DB / 网络 / 外部服务故障 | 高（可重试） |

自动分类规则：`context.DeadlineExceeded / Canceled` → `CodeTimeout`，其余 → `CodeProcessing`；
用户可用 `WithCode` 在 process 内覆盖。错误回调（`onError` / `OnError`）收到的都是包装后的
`*StageError`，便于按 `Code` 做日志分级、指标分类、死信路由等针对性处理。

### 8.5 死信队列（D-23，已实现）

处理失败且最终未进入下游的数据**不丢弃**，投递到死信队列供事后查询 / 重放 / 人工介入：

```
        process 失败（FailFast 触发 / Collect / RetryFallback 重试耗尽+降级失败）
input ──→ Stage ──┬──→ output（成功）
                  └──→ DeadLetterSink（失败，不丢）
```

**落盘抽象**——`DeadLetterSink` 接口，框架不关心落到哪里：

```go
type DeadLetterSink interface {
    Write(rec DeadLetterRecord) error // 并发调用，实现必须线程安全
    Close() error                     // Pipeline 关闭时刷盘
}
```

| 实现 | 说明 |
|------|------|
| `JSONLDeadLetterSink`（默认） | `{Dir}/{stageName}.dl.jsonl`，每行一条 JSON 死信记录 |
| 用户自定义 | 实现接口（数据库 / MQ / Kafka 等），框架零改动 |

**记录结构**：`TS` / `Stage` / `Code`（错误分类）/ `Input`（json.RawMessage）/ `ErrMsg` / `Latency` / `Retried`（重试次数）。
Input 序列化：`DeadLetterConfig.MarshalInput` 自定义回调优先，默认 `json.Marshal`，
无法序列化（chan/func 等）时降级为 `fmt.Sprintf("%+v")` 文本。

**接入**——`PipelineConfig.DeadLetter *DeadLetterConfig`（nil 即不启用，零开销）：

```go
pl := pipeline.New[int,int](pipeline.PipelineConfig{
    LogDir:     "./logs",
    DeadLetter: &pipeline.DeadLetterConfig{Dir: "./logs"}, // Sink 为 nil → 默认 JSONL
})
// 自定义落盘：
pl := pipeline.New[int,int](pipeline.PipelineConfig{
    DeadLetter: &pipeline.DeadLetterConfig{Sink: myDBSink},
})
```

**消费与重放**：

```go
// 读走某 Stage 全部死信记录（JSONL 场景）
recs, _ := pl.DrainDeadLetters("numbers")

// 重放：死信输入作为新 Pipeline 的输入源（复用背压/错误策略）
replay := &pipeline.DeadLetterReplaySource[int]{
    Reader:    pipeline.NewJSONLDeadLetterReader("./logs"),
    Stage:     "numbers",
    Unmarshal: func(raw json.RawMessage) (int, error) { /* 断言回 T1 */ },
}
pl2.Input(replay)
```

**行为边界**：
- 死信写入失败仅打印告警，不影响主流程（尽力而为的补充通道）；
- `DeadLetterConfig.Sink` 非 nil（用户提供）时，sink 的 Close 由用户负责；
  默认 JSONL sink 由框架在 Stage.Close 时自动关闭；

### 8.6 条件汇聚（D-26，Keyed Fan-in）

`MergeNode` 提供 Fan-out 的对称能力：多个并行分支的结果按业务 key 凑齐后合并，交给下游。

```
                    ┌─ branch-A（库存）─┐
root ──→ fanout ────┼─ branch-B（支付）─┼──→ MergeNode ──→ 下游 Stage
                    └─ branch-C（风控）─┘        │
                                          MergeKey("order-A") 凑齐 3 条 → 合并
```

**核心语义**：一条数据 = 一个业务实体的一个分支结果；同一 `MergeKey()`（订单 ID / 用户 ID）的 N 个分支结果**全部到达后**才允许合并。不足则暂存 map；凑齐则整批取出送合并工作池并立即删除 key；超时未凑齐视为泄漏清理。

**接入方式**：

```go
// 1. 分支（现有 NextStage，routeFn 传 nil）
b1 := root.NextStage("inv", cfg, nil, fnInv)
b2 := root.NextStage("pay", cfg, nil, fnPay)
b3 := root.NextStage("risk", cfg, nil, fnRisk)

// 2. MergeNode（泛型类型参数 T Keyable，join 输出类型为 T）
join := pipeline.NewMergeNode("merge-order", pipeline.JoinConfig[orderInfo]{
    Size: 3, MergeTimeout: 10 * time.Minute, DeadLetter: mySink,
}, func(ctx context.Context, batch []orderInfo) (orderInfo, error) {
    return combine(batch[0], batch[1], batch[2])
})
join.Wire(b1).Wire(b2).Wire(b3)

// 3. 生命周期挂到各分支（Attach 仅注册生命周期/拓扑，不参与数据转发）
b1.Attach(join); b2.Attach(join); b3.Attach(join)

// 4. 下游用 NextStage 创建（自动管理生命周期，无需手动 Start/Close）
//    NextStage 自动调用 join.To(post) 完成拓扑接线。
post := join.NextStage("audit", cfg, nil, fnPost)
post.NextStage("report", cfg, nil, fnReport)

// 5. 拓扑展示：RenderGraph(root) 即可渲染完整图（root 树 + merge 段 + 下游链）
fmt.Println(pipeline.RenderGraph(root))
```

**内部实现要点**：

**内部实现要点**：

| 机制 | 说明 |
|------|------|
| `pending` map | `branchArr[]bool` 位图防同 key 多批次错乱；`arrivedCnt == Size` 即凑齐 |
| 单收集 goroutine | select 多源 + sweep.C + ctx.Done，**独占 map 零锁** |
| 合并工作池 | mergeCh 有界队列 + N 个 worker 执行 merge，收集不被计算阻塞 |
| 过期清理 | `MergeTimeout` 未凑齐 → sweep 扫描 → `OnLeak` 回调 + 可选死信 + 删除 |
| **引用计数关闭** | `sync.Once` 不够：分支按序关闭时，真关闭须等**最后一个父**（此时全部分支已停，收集自然退出），否则收集会阻塞在未关分支的 channel 上 |
| `Attach` | `Stage.subStages` 新增生命周期子节点（不创建 fanout 分支），由父递归 Start/Close/Describe/GraphTD |
| **下游 routeFn** | `NextStage` 可多次调用创建多个下游分支，每个分支独立通道 + `routeFn` 过滤（D-25）；合并结果经 fan-out 分发 goroutine 按路由条件投递到匹配分支 |
| **MergeNode 可观测性** | `JoinConfig` 支持 `SlowThreshold`（merge 慢日志）/ `RateLimiter`（merge 前限流）/ `OnMergeError`（merge 失败回调）；MergeNode 接入 `stageMonitor`，Metrics/MetricsServer 可观测 merge 吞吐与延迟（D-30） |

**边界**：Wire 数 ≠ Size 报错；同 key 超量到达忽略+告警；输出背压经 mergeCh 反压到分支（天然背压）；Close 时残批走 OnLeak+死信不丢弃。

---

## 9. 使用示例

```go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "strconv"
    "syscall"
    "time"

    "pipeline"
)

func main() {
    // 定义 Stage：NextStage 链式连接（D-20）
    // stage1: 过滤非数字字符串
    s1 := pipeline.NewStage("stage-validate", pipeline.StageConfig{
        Workers: 2, OutCap: 16, Timeout: 5 * time.Second,
    }, nil, nil, func(ctx context.Context, in string) (int, error) { // 第 4 参 routeFn=nil 放行
        out, err := strconv.Atoi(in)
        if err != nil {
            return 0, fmt.Errorf("非数字字符串: %q", in)
        }
        return out, nil
    })
    // stage2: 十进制 int 转 8 进制字符串（NextStage 自动连线 s2.input = s1.output）
    s2 := s1.NextStage("stage-to-octal", pipeline.StageConfig{
        Workers: 2, OutCap: 16,
    }, nil, func(ctx context.Context, in int) (string, error) { // routeFn=nil 放行
        return strconv.FormatInt(int64(in), 8), nil
    })
    // stage3: 打印输出
    s2.NextStage("stage-print", pipeline.StageConfig{
        Workers: 2, OutCap: 16,
    }, nil, func(ctx context.Context, in string) (string, error) {
        fmt.Println("octal:", in)
        return in, nil
    })

    // 构建 Pipeline：NextStage 链式连接（D-20）
    pl := pipeline.New[string, int](pipeline.PipelineConfig{
        Name:            "example-pipeline",
        InputBufferSize: 100, // 首节点 inchan 缓冲区
        LogDir:          "./logs",
        LogEnabled:      true,
    }).
        AddStage(s1).
        Input(&pipeline.MockSource[string]{Data: []string{"10", "20", "30"}}).
        ErrorPolicy(pipeline.ErrPolicy{
            Mode:       pipeline.ErrModeRetryFallback,
            MaxRetry:   3,
            RetryDelay: time.Second,
            Fallback:   func(ctx context.Context, in any) (any, error) { return "[fallback]", nil },
        })

    // 运行：阻塞直到关闭完成（递归 Start/Close）
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    go func() {
        <-ctx.Done()
        if err := pl.Close(10 * time.Second); err != nil {
            fmt.Fprintln(os.Stderr, "close error:", err)
        }
    }()

    if err := pl.Run(ctx); err != nil {
        fmt.Fprintln(os.Stderr, "run error:", err)
    }
}
```

### 9.1 拓扑校验（D-28，构建期安全网）

`Pipeline.Validate()` 在构建期检查拓扑合法性，将配置错误从"线上数据消失才排查"提前到"写代码时立即暴露"：

```go
pl := pipeline.New[string, int](pipeline.PipelineConfig{Name: "demo"}).
    AddStage(root).Input(src)

// 构建期校验：返回所有错误（一次性暴露，不逐个卡）
if errs := pl.Validate(); len(errs) > 0 {
    for _, e := range errs {
        fmt.Println("拓扑错误:", e)
    }
    return
}
pl.Run(ctx)
```

| 校验项 | 说明 | 错误示例 |
|--------|------|---------|
| 数据流无环 | 从 root 沿数据流出边 DFS 三色标记，遇灰节点即环 | `cycle: s1 -> s2 -> s1` |
| MergeNode 分支数匹配 | `len(Wire) == JoinConfig.Size` | `merge 'x': wired=2 size=3` |
| MergeNode 生命周期已挂载 | 每个 Wire 的分支必须 `Attach` 了该 merge | `merge 'x' not attached to branch 'b1'` |
| 分支不重复 | 同一分支 `Wire` 两次 | `duplicate branch in merge 'x'` |
| 下游不重复 | 同一 Stage 被 `To/NextStage` 登记两次 | `duplicate downstream 'y'` |
| 无孤立节点 | 被引用但不在 root 数据流图中 | `unreachable: ghost` |

**内部实现**：`Stage.dataSubs []Stager` 记录仅 NextStage 创建的数据流子节点（Attach 不记录），
配合 `topoNode` 接口（`topoDataOut`/`topoLifeOut`/`topoBranches`/`topoBranchReqs`）统一访问
Stage 与 MergeNode 的拓扑结构；遍历用 `map[Stager]int` 三色标记。

---

## 10. 未来扩展规划（预留扩展点）

当前已实现**串行链 + 树形 Fork 多分支 + 条件路由 + Keyed 汇聚 + 拓扑校验**（D-20/D-21/D-25/D-26/D-28），以下能力属于后续迭代：

| 版本 | 扩展 | 说明 |
|------|------|------|
| 1.1 | Prometheus 监控 | 独立可选子模块 `metrics/prometheus`（保持核心零依赖），`Monitor.Metrics()` 快照 → prometheus Collector，含吞吐/Gauge/Counter/Summary（P50/P99） |
| 1.2 | 内置调度器 | `Pipeline.Schedule(cron)` 定时触发运行，支持 cron 表达式、防重叠、单次超时、错时捕获；标准库无 cron 解析器，需自实现 ~100 行 |
| 2.0 | 持久化 / StateStore | `StateStore` 接口 + 文件默认实现 + InputSource 进度恢复（At-least-once）；已评估暂缓，用户可自行在 InputSource 内实现 offset |
| 待定 | OTel 集成 | `StageHooks` 已支持手动注入 trace span，是否内置 OTel 与零依赖承诺冲突，暂缓 |
| 待定 | DAG 一般化 | 从树形扩展到任意有向无环图（多入多出）；改造成本极高，可用多次 MergeNode 组合替代 |

> 原则：**Stage 层保持纯业务，拓扑与并发调度全部下沉到 Pipeline 层**。当前已实现串行链与树形 Fork（多分支），未来可扩展为一般 DAG 图。

---

## 11. 变更管理流程（强制约定）

> ⚠️ **本节是项目工作流红线，任何参与本项目的开发都必须遵守。**

```
提出需求（新增 / 修改 / 删除功能）
        │
        ▼
   ① 更新 ARCHITECTURE.md
      - 新增/修改「设计决策记录（ADR）」条目
      - 同步修改相关章节（架构图 / 类型设计 / 时序 / 示例）
        │
        ▼
   ② 用户确认文档变更
        │
        ▼
   ③ 编写代码实现（遵循文档）
        │
        ▼
   ④ 验证（测试 / 构建 / 运行）
        │
        ▼
   ⑤ 完成，文档与代码状态一致
```

**红线条款**

1. 任何功能变更必须先改文档，后写代码；
2. 文档与代码不一致时，**以文档为准**，先修正代码；
3. 重大方案调整（如引入 Fork / Join）必须新增 ADR 条目并写明决策理由；
4. 实现过程中发现文档有误，先更新文档再改代码，不得绕过。

---

## 12. 项目文件架构（组件库形态）

> D-10：本库是**组件库（Library）**，交付给其他 Go 项目通过 `import "pipeline"` 使用，不是可执行程序。文件架构围绕组件库形态设计：对外 API 收口在根包，内部实现以**不导出标识符**（小写类型/函数）隔离于根包，示例独立于库代码。

### 12.1 文件目录结构

```
pipeline/
├── go.mod                 # module github.com/YUJIAJING0408/pipeline / go 1.27（组件库，无 main 包）
├── go.sum                 # 依赖校验
├── LICENSE                # MIT 许可证
├── version                # 当前版本号（CI 自动打 tag 时读取）
├── ARCHITECTURE.md        # 架构设计文档（权威，变更先改这里）
├── README.md              # 库简介 + 快速开始（指向 examples）
├── doc.go                 # package pipeline 包级文档（godoc 入口）
├── .github/
│   └── workflows/
│       └── ci.yml         # GitHub Actions：push 到 main 时自动测试 + 打 tag
│
│   ── 根包（对外公开 API + 内部实现） ──
├── stage.go               # Stage 泛型 struct / StageConfig / NewStage / Start / Close（D-11~D-16）
├── workerPool.go          # 内部泛型工作池（不导出，D-15）
├── pipeline.go            # Pipeline 编排层（New / AddStage / Input / Run / Close，D-17/D-18）
├── input.go               # InputSource 接口、MockSource
├── errors.go              # ErrMode、ErrPolicy、ErrorCode / StageError（D-04）
├── logger.go              # StageLogger JSON 结构化日志（D-08）
├── monitor.go             # StageMonitor 环形缓冲分位数 / Monitor 聚合（D-02）
├── metrics.go             # MetricsServer：标准库 HTTP + SSE 实时指标面板（7.4）
├── index.html             # MetricsServer 前端页面（//go:embed 嵌入 metrics.go）
├── deadletter.go          # DeadLetterSink 接口 + JSONL 实现 + ReplaySource（D-23）
├── join.go                # MergeNode 条件汇聚：Keyed Fan-in（D-26，Attach 生命周期子节点）
├── validate.go            # Pipeline.Validate() 拓扑校验（D-28：环/分支/孤立/重复检测）
├── ratelimit.go           # RateLimiter 令牌桶限流器（D-29：Allow/Wait）
│
│   ── 测试（根包同目录 _test.go） ──
├── stage_test.go          # Stage 运行时测试（Start/Close/ctx 链/路由）
├── workerPool_test.go     # 泛型工作池测试（D-15/错误路径/Hooks）
├── pipeline_test.go       # 构建 / Run / Close / 连线测试
├── errors_test.go         # 三模式错误策略 + 错误类型体系测试
├── logger_test.go         # JSON 结构化日志级别 / 过滤测试
├── monitor_test.go        # 时间统计 / 汇总报告测试
├── metrics_test.go        # 分位数 / SSE / 吞吐 / XSS / E2E 实时指标测试
├── deadletter_test.go     # JSONL / 自定义 sink / marshal 降级 / 重放测试
├── join_test.go           # 汇聚凑齐 / 缺分支泄漏 / 引用计数关闭 / E2E 测试
├── validate_test.go       # 拓扑校验：环 / 分支数 / Attach / 重复 / 孤立 / 多错误
├── ratelimit_test.go      # 限流器：匀速 / 突发 / 并发 / Wait 超时 / Stage 集成
├── integration_test.go    # 高吞吐 / 背压 / panic 恢复 / goroutine 泄漏等集成测试
├── benchmark_test.go      # 单 Stage / 链式 / Worker 扩展性 / IO 密集型 Benchmark（0 allocs）
│
└── examples/              # 可执行示例（唯一允许 package main 的地方）
    ├── basic/             # 最小可运行示例：构建 → Run → 优雅关闭
    │   ├── main.go
    │   └── README.md
    ├── real/              # 复杂树形拓扑 + 定时数据源 + 吞吐统计
    │   ├── main.go
    │   └── README.md
    ├── metrics/           # MetricsServer 演示：Pipeline 运行 + SSE 实时仪表盘
    │   └── main.go
    └── merge/             # MergeNode 演示：三分支并行 → Keyed 汇聚 → 下游
        ├── main.go
        └── README.md
```

### 12.2 文件归属原则

| 文件 | 归属包 | 导出性 | 职责 |
|------|--------|--------|------|
| `stage.go` / `pipeline.go` / `input.go` / `errors.go` / `logger.go` / `monitor.go` / `metrics.go` / `deadletter.go` / `join.go` / `validate.go` / `ratelimit.go` | 根包 `pipeline` | 全部导出 | 对外 API 面 |
| `workerPool.go` | 根包 `pipeline` | 不导出 | Stage 内部泛型工作池（D-15） |
| `index.html` | 根包 `pipeline` | embed 内嵌 | MetricsServer 前端页面（`//go:embed`） |
| `*_test.go`（含 benchmark_test.go） | 根包（同包测试） | 测试 | 白盒测试，可访问内部字段 |
| `examples/*/` | `package main` | 示例 | 演示 API 用法 |

### 12.3 组件库约束（红线）

1. **根目录不允许出现 `package main`**，可执行程序只在 `examples/` 下；
2. 对外 API 全部集中在根包 `pipeline`，内部实现（如 `workerPool`、`deadLetterWriter`）以**不导出标识符**隔离于根包，防止使用者误 import 内部细节；
3. 所有导出类型 / 函数必须有 godoc 注释（golint 强制），保证 `go doc` 质量；
4. 测试与被测代码同包（根包 `_test.go`），保证白盒可测性；
5. 不引入第三方运行时依赖（保持零依赖组件库，便于被任意项目引用）。

### 12.4 实现顺序（文件架构先行）

用户约定：进入实现阶段后，**先完成整个项目文件架构**（即本节的目录结构 + go.mod / doc.go / README 骨架），再逐文件填充业务实现。

**当前迭代范围（D-12）**：核心链路完整实现，包括：Pipeline / Stage / NextStage 链式 / subStages 递归 / 广播 Fan-out / 条件路由（D-25）/ Keyed 汇聚（D-26）/ Sink 消费 / 慢处理计时 / JSON 结构化日志 / 错误类型体系 / 死信队列 / 生命周期钩子 / 实时指标面板；测试 99 个（含集成/并发/泄漏/SSE 手动验证/Benchmark 0 allocs）。

---

## 附录 A：关键字汇总

| 关键字 | 来源 | 含义 |
|--------|------|------|
| D-01 ~ D-15 | 需求讨论 | 设计决策条目 |
| 级联关闭 | 需求 | Pipeline 关闭时从上到下逐层排空并传递关闭信号 |
| 排空（Drain） | 需求 | Stage 处理完输入缓冲区中的剩余数据 |
| 背压（Backpressure） | 设计 | 缓冲区满时上游写入阻塞，自然限流 |
| 组件库（Library） | 需求 | 本库作为 Library 供其他项目 import 使用，不含 package main |