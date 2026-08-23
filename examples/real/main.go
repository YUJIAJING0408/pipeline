// Command real 演示 pipeline 组件库的树形流水线（Fork 分叉）+ 吞吐量统计。
//
// 场景：电商订单处理中心。
// 定时数据源按速率生产随机订单（唯一递增 ID）→ 根节点解析 →
// 广播分发到三个分支（库存 / 支付 / 风控），每分支再挂子 Stage。
// 后台协程用原子计数统计每个 Stage 的每秒吞吐量。
//
// 运行：
//
//	cd examples/real && go run .
package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/example/pipeline"
)

// OrderInfo 解析后的订单信息（根节点输出，各分支共享类型）。
type OrderInfo struct {
	ID       string
	Product  string
	Quantity int
	Amount   float64
}

// branchResult 记录各分支处理结果（简化：统一用字符串 + 状态）。
type branchResult struct {
	Branch string
	Detail string
}

// timedSource 定时生产随机订单的输入源：按 ratePerSec 速率持续投递，
// 订单 ID 唯一递增，字段随机生成。
type timedSource struct {
	ratePerSec float64
	idCounter  atomic.Int64
}

// Start 实现 InputSource：按速率周期性生成订单并发送到 out。
func (s *timedSource) Start(ctx context.Context, out chan<- string) error {
	interval := time.Duration(float64(time.Second) / s.ratePerSec)
	products := []string{"laptop", "mouse", "keyboard", "monitor", "ssd"}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			id := s.idCounter.Add(1)
			product := products[rand.Intn(len(products))]
			qty := rand.Intn(5) + 1
			amount := float64(rand.Intn(20000)+500) / 10 // 50.0 ~ 2049.9
			out <- fmt.Sprintf("ORD-%05d:%s:%d:%.1f", id, product, qty, amount)
		}
	}
}

// throughput 统计所有 Stage 的处理吞吐量（预注册固定槽位 + 无锁原子，
// 避开 map+mutex 在 1 万/秒高吞吐下的锁竞争）。
type throughput struct {
	names []string
	seq   []*atomic.Int64 // 与 names 一一对应，下标直达
}

func newThroughput(names ...string) *throughput {
	t := &throughput{}
	for _, name := range names {
		t.names = append(t.names, name)
		t.seq = append(t.seq, &atomic.Int64{})
	}
	return t
}

// idx 返回 name 对应槽位下标（-1 表示未注册）。
func (t *throughput) idx(name string) int {
	for i, n := range t.names {
		if n == name {
			return i
		}
	}
	return -1
}

func (t *throughput) inc(name string) {
	if i := t.idx(name); i >= 0 {
		t.seq[i].Add(1)
	}
}

// report 每秒打印一次各 Stage 吞吐量（当前秒增量）。
func (t *throughput) report(ctx context.Context) {
	prev := make([]int64, len(t.seq))
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			parts := make([]string, 0, len(t.names))
			for i, c := range t.seq {
				cur := c.Load()
				perSec := cur - prev[i]
				prev[i] = cur
				parts = append(parts, fmt.Sprintf("%s=%d/s", t.names[i], perSec))
			}
			fmt.Printf("[吞吐量] %s\n", strings.Join(parts, "  "))
		}
	}
}

// parseOrder 将 "ORD-00001:laptop:2:1999.9" 形式的原始字符串解析为 OrderInfo。
func parseOrder(ctx context.Context, raw string) (OrderInfo, error) {
	parts := strings.Split(raw, ":")
	if len(parts) != 4 {
		return OrderInfo{}, fmt.Errorf("非法订单格式: %q", raw)
	}
	qty, err := strconv.Atoi(parts[2])
	if err != nil {
		return OrderInfo{}, fmt.Errorf("非法数量: %q", parts[2])
	}
	amount, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return OrderInfo{}, fmt.Errorf("非法金额: %q", parts[3])
	}
	return OrderInfo{ID: parts[0], Product: parts[1], Quantity: qty, Amount: amount}, nil
}

func main() {
	rate := 100000.0 // 每秒生产 10000 条订单
	src := &timedSource{ratePerSec: rate}
	tp := newThroughput("root-parse", "branch-inventory", "sub-stockwarn", "branch-payment", "sub-receipt", "branch-risk", "sub-audit")

	// ── 1. 根节点：解析订单（string → OrderInfo）────────────────────────────
	root := pipeline.NewStage("root-parse", pipeline.StageConfig{
		Workers: 2, OutCap: 32, Timeout: 5 * time.Second,
	}, nil, nil, func(ctx context.Context, raw string) (OrderInfo, error) {
		info, err := parseOrder(ctx, raw)
		if err == nil {
			tp.inc("root-parse")
		}
		return info, err
	})

	// ── 2. 分支 A：库存处理（OrderInfo → branchResult）─ 子分支：库存预警 ──
	inventory := root.NextStage("branch-inventory", pipeline.StageConfig{
		Workers: 2, OutCap: 16,
	}, nil, func(ctx context.Context, o OrderInfo) (branchResult, error) {
		tp.inc("branch-inventory")
		return branchResult{Branch: "inventory", Detail: fmt.Sprintf("%s x%d 库存充足", o.Product, o.Quantity)}, nil
	})
	inventory.NextStage("sub-stockwarn", pipeline.StageConfig{
		Workers: 1, OutCap: 8,
	}, nil, func(ctx context.Context, r branchResult) (branchResult, error) {
		tp.inc("sub-stockwarn")
		return r, nil
	}).Sink(func(ctx context.Context, r branchResult) {}) // 叶子：结果直接消费，防背压死锁

	// ── 3. 分支 B：支付处理（OrderInfo → branchResult）─ 子分支：生成回执 ──
	payment := root.NextStage("branch-payment", pipeline.StageConfig{
		Workers: 2, OutCap: 16,
	}, nil, func(ctx context.Context, o OrderInfo) (branchResult, error) {
		tp.inc("branch-payment")
		return branchResult{Branch: "payment", Detail: fmt.Sprintf("%s 支付 ¥%.2f", o.ID, o.Amount)}, nil
	})
	payment.NextStage("sub-receipt", pipeline.StageConfig{
		Workers: 1, OutCap: 8,
	}, nil, func(ctx context.Context, r branchResult) (branchResult, error) {
		tp.inc("sub-receipt")
		return r, nil
	}).Sink(func(ctx context.Context, r branchResult) {})

	// ── 4. 分支 C：风控处理（OrderInfo → branchResult）─ 子分支：人工审计 ──
	risk := root.NextStage("branch-risk", pipeline.StageConfig{
		Workers: 2, OutCap: 16,
	}, nil, func(ctx context.Context, o OrderInfo) (branchResult, error) {
		tp.inc("branch-risk")
		level := "低"
		if o.Amount > 1000 {
			level = "高"
		}
		return branchResult{Branch: "risk", Detail: fmt.Sprintf("%s 风控: %s", o.ID, level)}, nil
	})
	risk.NextStage("sub-audit", pipeline.StageConfig{
		Workers: 1, OutCap: 8,
	}, nil, func(ctx context.Context, r branchResult) (branchResult, error) {
		tp.inc("sub-audit")
		return r, nil
	}).Sink(func(ctx context.Context, r branchResult) {})

	// ── 5. 组装 Pipeline：根节点 + 定时输入源 ──────────────────────────────
	pl := pipeline.New[string, OrderInfo](pipeline.PipelineConfig{
		Name:            "order-center",
		InputBufferSize: 16,
		LogDir:          "./logs",
		LogEnabled:      true,
	}).
		Input(src).
		AddStage(root)

	fmt.Println("── 树形流水线拓扑 ──")
	fmt.Println(pl.GraphTD())
	fmt.Printf("── 定时生产源：%s 条/秒，运行 20 秒后自动退出，Ctrl+C 可提前结束 ──\n", strings.TrimSuffix(strconv.FormatFloat(rate, 'f', 1, 64), ".0"))

	// ── 6. 运行：5 秒自动退出（或信号提前触发级联关闭）+ 吞吐量统计 ─────────
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		select {
		case <-sigCh:
			fmt.Println("收到退出信号，开始关闭…")
		case <-time.After(20 * time.Second):
			fmt.Println("运行 20 秒，开始关闭…")
		}
		cancelRun()
	}()

	go tp.report(runCtx)
	if err := pl.Run(runCtx); err != nil {
		fmt.Fprintln(os.Stderr, "run error:", err)
	}
	if err := pl.Close(5 * time.Second); err != nil {
		fmt.Fprintln(os.Stderr, "close error:", err)
	}
	fmt.Println("Pipeline 正常退出")
}
