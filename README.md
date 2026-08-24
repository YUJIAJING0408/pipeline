# pipeline

通用、类型安全的 Go 流水线（Pipeline）业务框架，以组件库形态交付。

将复杂业务拆解为多个 **Stage** 串联，通过 `NextStage` 做类型安全的链式连接
（编译器约束父 Stage 输出类型 = 子 Stage 输入类型），
支持级联优雅关闭、JSON 结构化日志、错误策略与实时指标面板。

## 为什么用 Pipeline，而不是 if/else/for？

业务代码天然是多步骤、多分支的。第一步解析输入 → 校验 → 查库存 → 算价格 → 扣库存 → 出单。用 if/else/for 写出来很容易，但到一定规模后问题就暴露了：

**① 数据流不透明**——数据在函数间怎么传的、每个步骤的耗时、错误率和吞吐量，全靠打日志拼凑。没有统一的监控点，排查问题靠猜。

**② 错误处理分散**——每个步骤的失败处理散落在各个 if 分支里，有的重试、有的跳过、有的直接报错，策略不一致，改一处漏一处。

**③ 并发要自己写**——需要并行处理时（同时查库存、支付、风控），手动开 goroutine + WaitGroup + channel，代码量翻倍，还要考虑 goroutine 泄漏和优雅关闭。

**④ 关闭是噩梦**——收到退出信号后，正在处理的数据怎么办？在途数据要不要等？关闭顺序怎么保证？手动写 shutdown 逻辑几乎每次都有 bug。

**Pipeline 把这些问题变成框架语义**：数据流就是 Stage 链，每条数据的处理耗时/成功/失败自动记录；错误策略统一配置（快速失败/容错/重试降级）；并行分支通过 `NextStage` 的 fan-out 广播自动完成，不需要手动管理 goroutine；级联优雅关闭是递归的，`Pipeline.Close` 一行调用保证在途数据排空，不留悬挂协程。

简单说：**if/else/for 描述"怎么做"，Pipeline 描述"做什么"**——你定义每个 Stage 的业务逻辑，框架负责编排、并发、错误恢复和优雅关闭。

## 特性

- **`NextStage` 链式连接**：`Stage[T1, T2].NextStage[T3](name, cfg, fn)` 创建子 Stage，
  自动连线 `s.input = 父.output`，输出类型 `T2` 强制作为子 Stage 输入类型
- **递归 Start/Close**：每个 Stage 持有 `subStages`，Start 先启动子 Stage 再启动自身，
  Close 先关闭自身再关闭子 Stage——从前往后逐级传播
- **`Pipeline[T1, T2]` 编排层**：持有根 Stage，管理输入源注入（`T1` = 输入类型、
  `T2` = 根 Stage 输入类型），`AddStage` 设置根节点
- **级联优雅关闭**：启动与关闭均从前往后，前驱完成关闭后经 `forwardCtx` 通知后继
- **Channel 数据流通**：首节点 inchan 缓冲区由 `PipelineConfig.InputBufferSize` 配置，天然背压
- **泛型 WorkerPool**：每个 Stage 内部并发处理，支持单条超时
- **三模式错误策略**：FailFast / Collect / Retry+Fallback，`OnError` 回调，process panic 自动 recover；
  可全局配置（`Pipeline.ErrorPolicy()`）或按 Stage 覆盖（`StageConfig.ErrPolicy`）
- **重试指数退避**：`RetryBackoff` 乘数（100ms→200ms→400ms），上限 1h（D-31）
- **错误类型体系**：`StageError` 分类（超时/非法输入/业务/系统），`errors.Is/As` 判型，`WithCode` 标注
- **死信队列**：失败数据投递 `DeadLetterSink`（默认 JSONL 落盘），`DrainDeadLetters` 读取 + 重放
- **条件路由**：`NextStage` 的 `routeFn` 参数按条件分发给匹配分支（D-25）
- **Keyed 汇聚**：`MergeNode` 按 `MergeKey` 凑齐多分支结果合并（D-26），支持多下游 + 路由
- **生命周期钩子**：`StageHooks`（`OnBeforeProcess`/`OnAfterProcess`）注入 trace / 审计 / 限流
- **JSON 结构化日志**：每 Stage 独立文件，四级日志级别（debug/info/warn/error）+ 结构化字段
- **实时指标面板**：`MetricsServer` 基于标准库 HTTP + SSE 推送吞吐 / P50 / P99 / 队列深度 / 阻塞时长；
  可独立端口运行，也可挂载到用户 HTTP 服务；面板含吞吐趋势折线图
- **背压可视化**：`Monitor.Metrics()` 暴露队列深度与平均/累计阻塞时长，快速定位瓶颈（D-27）
- **拓扑校验**：`Pipeline.Validate()` 构建期检测环 / MergeNode 配置错误 / 孤立节点（D-28）
- **内置限流器**：令牌桶 `RateLimiter`（Allow 丢弃式 / Wait 背压式），按 Stage 保护下游慢依赖（D-29）；MergeNode 同样支持（D-30）
- **零第三方依赖**：纯标准库实现

## 快速开始

```go
package main

import (
    "context"
    "fmt"
    "strconv"
    "time"

    "github.com/YUJIAJING0408/pipeline"
)

func main() {
    // 1. 定义 Stage 链（NextStage 逐级连接，类型由编译器强制匹配）
    s1 := pipeline.NewStage("stage-validate", pipeline.StageConfig{
        Workers: 2, OutCap: 16, Timeout: 5 * time.Second,
    }, nil, nil, func(ctx context.Context, in string) (int, error) {
        return strconv.Atoi(in)
    })
    s2 := s1.NextStage("stage-to-octal", pipeline.StageConfig{
        Workers: 2, OutCap: 16,
    }, nil, func(ctx context.Context, in int) (string, error) {
        return strconv.FormatInt(int64(in), 8), nil
    })
    s2.NextStage("stage-print", pipeline.StageConfig{
        Workers: 2, OutCap: 16,
    }, nil, func(ctx context.Context, in string) (struct{}, error) {
        fmt.Println("octal:", in)
        return struct{}{}, nil
    })

    // 2. 组装 Pipeline：配置（D-18）+ 根 Stage + Mock 输入源
    pl := pipeline.New[string, int](pipeline.PipelineConfig{
        Name:            "quickstart",
        InputBufferSize: 16,
        LogDir:          "./logs",
        LogEnabled:      true,
    }).AddStage(s1).Input(&pipeline.MockSource[string]{Data: []string{"10", "20", "30"}})

    // 3. 运行（阻塞），ctx 取消后由 Close 完成级联优雅关闭
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()
    go func() {
        <-time.After(time.Second)
        cancel()
    }()

    if err := pl.Run(ctx); err != nil {
        panic(err)
    }
    if err := pl.Close(5 * time.Second); err != nil {
        panic(err)
    }
}
```

## 目录结构

```
├── stage.go          # Stage / StageConfig / NewStage / NextStage / Attach / Start / Close（对外 API）
├── workerPool.go     # Stage 内部泛型工作池（不导出）
├── pipeline.go       # Pipeline / PipelineConfig / New / AddStage / Run / Close / DrainDeadLetters（对外 API）
├── input.go          # InputSource / MockSource（对外 API）
├── errors.go         # ErrMode / ErrPolicy / StageError / 哨兵错误（对外 API）
├── logger.go         # StageLogger JSON 结构化日志（对外 API）
├── monitor.go        # StageMonitor 分位数 / 背压指标 / Monitor 汇总（对外 API）
├── metrics.go        # MetricsServer 实时指标面板（HTTP + SSE + 可挂载，对外 API）
├── index.html        # MetricsServer 前端页面（//go:embed 嵌入）
├── deadletter.go     # DeadLetterSink 接口 + JSONL 落盘 + 重放（对外 API）
├── join.go           # MergeNode 条件汇聚：Keyed Fan-in（对外 API）
├── version           # 版本号 + 更新项（CI 打 tag 读取）
├── .github/workflows/ci.yml  # GitHub Actions：push 自动测试 + 打 tag
└── examples/         # 可执行示例（basic/real/metrics/merge）
```

## 文档

- [架构设计](./ARCHITECTURE.md)（权威设计文档，含 ADR 决策记录）
- [examples/basic](./examples/basic/) 最小可运行示例
- [examples/real](./examples/real/) 树形分支 + 吞吐统计
- [examples/metrics](./examples/metrics/) 实时指标面板演示
- [examples/merge](./examples/merge/) Keyed 汇聚演示

## 状态

核心链路已跑通：`NextStage 链式连接 → 递归 Start/Close → 输入源注入 → 级联优雅关闭`。
JSON 结构化日志、三模式错误策略（含 panic recover、按 Stage 覆盖）、死信队列、条件路由（D-25）、
Keyed 汇聚（D-26）、生命周期钩子、背压可视化（D-27）、拓扑校验（D-28）、内置限流器（D-29）、MergeNode 可观测性（D-30）、重试退避（D-31）——121 个测试（含集成/并发/泄漏）全部通过，
Benchmark 零内存分配。零第三方依赖。