# merge 示例：汇聚合并（Fan-in / Keyed Join）

演示 pipeline 组件库的 **多合一** 能力：多个并行分支的结果按业务 key 凑齐后合并。

## 场景：订单结算中心

```
                                   ┌─ branch-inventory（库存检查，2ms）
ORD-00001 ── root-parse ── 广播 ──┼─ branch-payment（支付处理，5ms）──┐
                                   └─ branch-risk（风控审核，8ms）───┤
                                                                    ├── merge-settle ── sink-audit
                           同一订单 3 分支结果按订单 ID 凑齐后合并 ──┘
```

1. **Fan-out 广播**：`root.NextStage(...)` 3 次，同一订单被三条分支并行处理
2. **Keyed Join 汇聚**：`NewMergeNode(Size: 3)` 按 `MergeKey()`（订单 ID）凑齐
   3 条分支结果——**全部到齐才合并**，缺一条则等待（超时进 OnLeak）
3. **下游消费**：合并结果经 `merge.NextStage()` 自动接入下游（生命周期自动管理，无需手动 Start/Close）

## 运行

```bash
cd examples/merge && go run .
```

## 输出示例

```
── 汇聚流水线拓扑（Fan-in）──
graph TD
    S1["root-parse"] --> S2["branch-inventory"]
    S2["branch-inventory"] --> S3["merge-settle"]
    S3["merge-settle"] --> S4["sink-audit"]
    S1["root-parse"] --> S5["branch-payment"]
    S5["branch-payment"] --> S3["merge-settle"]
    S1["root-parse"] --> S6["branch-risk"]
    S6["branch-risk"] --> S3["merge-settle"]

  订单 ORD-00001 结算: [ok] inventory=库存充足, payment=扣款 ¥1999.90, risk=风控通过
  订单 ORD-00003 结算: [fail] inventory=库存充足, payment=扣款 ¥8999.00, risk=大额订单人工复核
  订单 ORD-00004 结算: [ok] inventory=库存充足, payment=扣款 ¥299.00, risk=风控通过
  订单 ORD-00002 结算: [fail] inventory=缺货, payment=扣款 ¥399.00, risk=风控通过
```

测试数据包含正常、缺货、风控拒绝三种路径，展示合并结果正确反映各分支状态。

## 关键 API

| API | 作用 |
|-----|------|
| `NewMergeNode[T Keyable](name, cfg, mergeFn)` | 创建合并节点，按 key 凑齐 cfg.Size 条 |
| `merge.Wire(branch)` | 接入一个分支（编译期约束输出类型实现 `Keyable`） |
| `branch.Attach(merge)` | 生命周期挂到分支（root 树递归启动/关闭 merge） |
| `merge.NextStage(name, cfg, nil, fn)` | **推荐**：创建下游 Stage，自动管理生命周期（Start/Close 由 merge 递归遍历） |
| `merge.InChan()` | 手动方式：合并结果输出通道，供 `NewStage(..., merge.InChan(), ...)` 消费 |
| `merge.To(sink)` | 拓扑接线（仅 GraphTD 绘制，NextStage 自动调用，手动场景需自行调用） |
| `pipeline.RenderGraph(root)` | 渲染完整图（fan-in 入边由分支递归自动画出） |