// Package pipeline 是一个通用、类型安全的 Go 流水线（Pipeline）业务框架。
//
// 核心概念（详见 ARCHITECTURE.md）：
//
//   - Stage[T1, T2]：单节点业务处理单元（泛型 struct + 函数注入），自带运行时
//     （WorkerPool / 私有 Channel / Context 链 / Start / Close / 慢日志 / 监控）；
//   - StageConfig：Workers/OutCap/Timeout/SlowThreshold/Hooks/ErrPolicy（按 Stage 覆盖错误策略）；
//   - Stage.NextStage[T3]：类型安全的链式连接，父 Stage 输出类型 T2 强制作为子 Stage
//     输入，可形成树形多分支拓扑；Fan-out 广播复制每条数据到所有子 Stage；
//     routeFn 支持条件路由（D-25）；SetDefaultBranch 兜底未匹配数据（D-32）；
//     MergeNode 支持 Keyed 汇聚（D-26）；
//   - Stage.Sink(fn)：叶子 Stage 将 process 结果直接交给消费函数，不写 output 通道，
//     避免树形叶子背压死锁（D-21）；
//   - Stage.Attach(st)：将外部节点（如 MergeNode）注册为生命周期子节点（D-26）；
//   - Pipeline[T1, T2]：编排层（T1=输入类型，T2=根 Stage 输入类型），AddStage 设置根
//     Stage，管理输入源注入与级联优雅关闭；ErrorPolicy() 全局策略、MetricsMonitor /
//     DrainDeadLetters / Output / OutputChan 等扩展（D-34）；
//   - Monitor：链路时间分析（各 Stage 记录 total/errors/latency + P50/P99 滑动窗口 +
//     背压指标 QueueDepth/BlockedTime + 错误分类 ErrCodes + 路由流量 RouteAccepted/Rejected），
//     GenerateSummary() / Format() / Metrics() 按需取用（D-02/D-27/D-35）；
//   - MetricsServer：实时指标面板，HTTP + SSE 推送吞吐/P50/P99/队列深度/阻塞时长；
//     可独立端口运行，也可通过 Handler/IndexHandler/MetricsHandler 挂载到用户 HTTP 服务；
//     面板含吞吐趋势折线图 + 整链路汇总行（D-33）（7.4）；
//   - 三模式错误策略：FailFast（取消 ctx 传播）/ Collect（记录跳过）/ RetryFallback
//     （重试+降级，支持 RetryBackoff 指数退避 D-31），通过 Pipeline.ErrorPolicy() 全局注入，
//     或 StageConfig.ErrPolicy 按 Stage 覆盖；
//   - 错误类型体系（D-04）：StageError 分类（CodeTimeout/CodeInvalidInput/CodeProcessing/
//     CodeSystem），errors.Is/As 判型，WithCode 标注；
//   - 死信队列（D-23）：DeadLetterSink 接口 + 默认 JSONL 落盘，支持重放 ReplaySource；
//   - 生命周期钩子（D-24）：StageHooks，OnBeforeProcess / OnAfterProcess；
//   - 结构化日志（D-08）：JSON 格式，四级日志级别，结构化字段；
//   - MergeNode（D-26）：按 MergeKey 凑齐 N 分支后合并，支持多下游 + routeFn 条件路由；
//     可观测性对齐 Stage（D-30）：SlowThreshold 慢日志 / RateLimiter 限流 / OnMergeError 回调 / stageMonitor 监控；
//   - Pipeline.Validate()（D-28）：构建期拓扑校验，检测环 / MergeNode 配置错误 / 孤立节点；
//   - RateLimiter（D-29）：内置令牌桶限流器（Allow 丢弃式 / Wait 背压式），StageConfig.RateLimiter 注入；
//   - InputSource[In] / MockSource[In]：链路初始输入，支持自定义定时源。
//
// 本库以组件库（Library）形态交付，供其他 Go 项目 import 使用，
// 不包含 package main（示例位于 examples/ 目录），保持零第三方运行时依赖。
package pipeline
