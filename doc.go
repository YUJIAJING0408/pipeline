// Package pipeline 是一个通用、类型安全的 Go 流水线（Pipeline）业务框架。
//
// 核心概念（详见 ARCHITECTURE.md）：
//
//   - Stage[T1, T2]：单节点业务处理单元（泛型 struct + 函数注入），自带运行时
//     （WorkerPool / 私有 Channel / Context 链 / Start / Close / 慢日志 / 监控）；
//   - Stage.NextStage[T3]：类型安全的链式连接，父 Stage 输出类型 T2 强制作为子 Stage
//     输入，可形成树形多分支拓扑；Fan-out 广播复制每条数据到所有子 Stage；
//     routeFn 支持条件路由（D-25）；MergeNode 支持 Keyed 汇聚（D-26）；
//   - Stage.Sink(fn)：叶子 Stage 将 process 结果直接交给消费函数，不写 output 通道，
//     避免树形叶子背压死锁；
//   - Stage.Attach(st)：将外部节点（如 MergeNode）注册为生命周期子节点（D-26）；
//   - Pipeline[T1, T2]：编排层（T1=输入类型，T2=根 Stage 输入类型），AddStage 设置根
//     Stage，管理输入源注入与级联优雅关闭；MetricsMonitor / DrainDeadLetters 等扩展；
//   - Monitor：链路时间分析（各 Stage 记录 total/errors/latency + P50/P99 滑动窗口），
//     GenerateSummary() / Format() / Metrics() 按需取用；
//   - MetricsServer：实时指标面板，HTTP + SSE 推送吞吐/P50/P99（7.4）；
//   - 三模式错误策略：FailFast（取消 ctx 传播）/ Collect（记录跳过）/ RetryFallback
//     （重试+降级），通过 Pipeline.ErrorPolicy() 全局注入；
//   - 死信队列（D-23）：DeadLetterSink 接口 + 默认 JSONL 落盘，支持重放；
//   - 生命周期钩子（D-24）：StageHooks，OnBeforeProcess / OnAfterProcess；
//   - 结构化日志（D-08）：JSON 格式，四级日志级别，结构化字段；
//   - InputSource[In] / MockSource[In]：链路初始输入，支持自定义定时源。
//
// 本库以组件库（Library）形态交付，供其他 Go 项目 import 使用，
// 不包含 package main（示例位于 examples/ 目录），保持零第三方运行时依赖。
package pipeline
