// Command merge 演示 pipeline 组件库的汇聚合并（Fan-in / Keyed Join）。
//
// 场景：订单结算中心。
// 根节点解析订单 → 广播分发到三个并行分支（库存 / 支付 / 风控）→
// 每个分支各自产出该订单的检查结果 → MergeNode 按订单 ID 凑齐三条结果、
// 合并为完整订单结算状态 → 下游入库。
//
// 与 examples/real 的区别：real 只展示一分多（Fan-out），各分支结果各自消费；
// 本示例展示多合一（Fan-in）——三个并行结果必须**全部到齐**才进入下一步。
//
// 运行：
//
//	cd examples/merge && go run .
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/YUJIAJING0408/pipeline"
)

// checkResult 单分支对某订单的检查结果（must 实现 pipeline.Keyable）。
type checkResult struct {
	OrderID string // 合并键：同一订单的三分支结果按此凑齐
	Branch  string // inventory / payment / risk
	Status  string // ok / fail
	Detail  string
}

// MergeKey 实现 Keyable：以订单 ID 作为合并分组键。
func (c checkResult) MergeKey() string { return c.OrderID }

// OrderInfo 解析后的订单信息（根节点输出，广播到各分支）。
type OrderInfo struct {
	ID      string
	Product string
	Amount  float64
}

// parseOrder 将 "ORD-00001:laptop:1999.9" 解析为 OrderInfo。
func parseOrder(ctx context.Context, raw string) (OrderInfo, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return OrderInfo{}, fmt.Errorf("非法订单格式: %q", raw)
	}
	amount, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return OrderInfo{}, fmt.Errorf("非法金额: %q", parts[2])
	}
	return OrderInfo{ID: parts[0], Product: parts[1], Amount: amount}, nil
}

func main() {
	// ── 1. 根节点：解析订单（string → OrderInfo）───────────────────────
	root := pipeline.NewStage("root-parse", pipeline.StageConfig{
		Workers: 2, OutCap: 16,
	}, nil, nil, parseOrder)

	// ── 2. 三个并行分支（Fan-out 广播）：同一订单分别做三种检查 ─────────
	inventory := root.NextStage("branch-inventory", pipeline.StageConfig{
		Workers: 2, OutCap: 16,
	}, nil, func(ctx context.Context, o OrderInfo) (checkResult, error) {
		time.Sleep(2 * time.Millisecond) // 模拟库存查询
		if o.Product == "out-of-stock" {
			return checkResult{OrderID: o.ID, Branch: "inventory", Status: "fail", Detail: "缺货"}, nil
		}
		return checkResult{OrderID: o.ID, Branch: "inventory", Status: "ok", Detail: "库存充足"}, nil
	})

	payment := root.NextStage("branch-payment", pipeline.StageConfig{
		Workers: 2, OutCap: 16,
	}, nil, func(ctx context.Context, o OrderInfo) (checkResult, error) {
		time.Sleep(5 * time.Millisecond) // 模拟支付网关
		return checkResult{OrderID: o.ID, Branch: "payment", Status: "ok", Detail: fmt.Sprintf("扣款 ¥%.2f", o.Amount)}, nil
	})

	risk := root.NextStage("branch-risk", pipeline.StageConfig{
		Workers: 2, OutCap: 16,
	}, nil, func(ctx context.Context, o OrderInfo) (checkResult, error) {
		time.Sleep(8 * time.Millisecond) // 模拟风控模型
		if o.Amount > 8000 {
			return checkResult{OrderID: o.ID, Branch: "risk", Status: "fail", Detail: "大额订单人工复核"}, nil
		}
		return checkResult{OrderID: o.ID, Branch: "risk", Status: "ok", Detail: "风控通过"}, nil
	})

	// ── 3. MergeNode：按订单 ID 凑齐 3 条分支结果，合并为结算状态 ────────
	merge := pipeline.NewMergeNode("merge-settle", pipeline.JoinConfig[checkResult]{
		Size:         3, // 每笔订单需 3 个分支全部到齐
		MergeTimeout: 5 * time.Second,
		OnLeak: func(key string, batch []checkResult) {
			fmt.Printf("⚠️  订单 %s 检查超时未凑齐（到 %d/3），进入人工处理\n", key, len(batch))
		},
	}, func(ctx context.Context, batch []checkResult) (checkResult, error) {
		// batch[0..2] = inventory / payment / risk（任意到达顺序，位图保证完整性）
		var parts []string
		allOK := true
		for _, r := range batch {
			parts = append(parts, fmt.Sprintf("%s=%s", r.Branch, r.Detail))
			if r.Status != "ok" {
				allOK = false
			}
		}
		return checkResult{
			OrderID: batch[0].OrderID,
			Branch:  "settled",
			Status:  boolStatus(allOK),
			Detail:  strings.Join(parts, ", "),
		}, nil
	})

	// Wire（接入 3 分支）+ Attach（生命周期挂到各分支，root 树递归启停）
	merge.Wire(inventory).Wire(payment).Wire(risk)
	inventory.Attach(merge)
	payment.Attach(merge)
	risk.Attach(merge)

	// ── 4. 下游：消费合并结果（NextStage 自动管理生命周期，无需手动 Start/Close）────
	merge.NextStage("sink-audit", pipeline.StageConfig{
		Workers: 1, OutCap: 16,
	}, nil, func(ctx context.Context, r checkResult) (checkResult, error) {
		fmt.Printf("  订单 %s 结算: [%s] %s\n", r.OrderID, r.Status, r.Detail)
		return r, nil
	}).Sink(func(ctx context.Context, r checkResult) {}) // 叶子消费，防背压
	// NextStage 自动调用 merge.To(sink) 完成拓扑接线，无需手动 To()。

	// ── 5. 组装 Pipeline + 固定订单源 ─────────────────────────────────
	pl := pipeline.New[string, OrderInfo](pipeline.PipelineConfig{
		Name:            "settle-center",
		InputBufferSize: 16,
	}).AddStage(root).Input(&pipeline.MockSource[string]{Data: []string{
		"ORD-00001:laptop:1999.9",
		"ORD-00002:out-of-stock:399.0", // 库存失败
		"ORD-00003:monitor:8999.0",     // 风控失败（>8000）
		"ORD-00004:keyboard:299.0",
	}})

	fmt.Println("── 汇聚流水线拓扑（Fan-in）──")
	// root 树递归：root→分支（NextStage）→merge（Attach 挂在分支下）→sink（To 挂在下游）
	fmt.Println(pipeline.RenderGraph(root))

	// ── 6. 运行：Ctrl+C 或 6 秒后退出 ─────────────────────────────────
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		select {
		case <-sigCh:
			fmt.Println("\n收到退出信号，开始关闭…")
		case <-time.After(6 * time.Second):
			fmt.Println("\n运行 6 秒，开始关闭…")
		}
		cancelRun()
	}()

	// 下游由 merge.NextStage 创建，自动跟随 MergeNode 生命周期（MergeNode 已 Attach 到各分支，
	// 分支随 root 树递归 Start/Close），因此无需手动启动 sink。
	if err := pl.Run(runCtx); err != nil {
		fmt.Fprintln(os.Stderr, "run error:", err)
	}
	if err := pl.Close(5 * time.Second); err != nil {
		fmt.Fprintln(os.Stderr, "close error:", err)
	}
	fmt.Println("已退出")
}

// boolStatus 将布尔合并结果转为 "ok"/"fail"。
func boolStatus(ok bool) string {
	if ok {
		return "ok"
	}
	return "fail"
}

// strconv 保留引用：金额精度校验（parseOrder 使用）。
var _ = strconv.ParseFloat