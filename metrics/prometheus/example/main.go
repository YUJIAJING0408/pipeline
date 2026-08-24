// Command example 演示树形多分支 Pipeline + Prometheus 子模块接入。
//
// 拓扑：
//
//	randSource（每秒 300 条）→ parse（string→OrderInfo, 0.5-2ms）
//	                              ├── inventory（库存检查, 2-4ms, 3% 错误率）
//	                              ├── payment（支付处理, 3-6ms, 5% 错误率）
//	                              └── risk（风控审核, 4-8ms, 2% 错误率）
//
// 运行：cd metrics/prometheus && go mod tidy && go run ./example
package main

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pipeline "github.com/YUJIAJING0408/pipeline"
	promexporter "github.com/YUJIAJING0408/pipeline/metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type OrderInfo struct {
	ID      string
	Product string
	Amount  float64
	Region  string
}

func (o OrderInfo) MergeKey() string { return o.ID }

type branchResult struct {
	Branch  string
	OrderID string
	Status  string
	Detail  string
}

type randSource struct {
	ratePerSec float64
	counter    int
}

func (s *randSource) Start(ctx context.Context, out chan<- string) error {
	interval := time.Duration(float64(time.Second) / s.ratePerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	products := []string{"laptop", "phone", "tablet", "watch", "headphone"}
	regions := []string{"cn", "us", "eu", "jp"}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.counter++
			product := products[rand.Intn(len(products))]
			amount := float64(rand.Intn(20000)+500) / 10
			region := regions[rand.Intn(len(regions))]
			select {
			case out <- fmt.Sprintf("ORD-%05d:%s:%.1f:%s", s.counter, product, amount, region):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func parseOrder(ctx context.Context, raw string) (OrderInfo, error) {
	time.Sleep(time.Duration(500+rand.Intn(1500)) * time.Microsecond)
	parts := strings.Split(raw, ":")
	if len(parts) != 4 {
		return OrderInfo{}, fmt.Errorf("invalid: %s", raw)
	}
	amount, _ := strconv.ParseFloat(parts[2], 64)
	return OrderInfo{ID: parts[0], Product: parts[1], Amount: amount, Region: parts[3]}, nil
}

func main() {
	root := pipeline.NewStage("parse", pipeline.StageConfig{Workers: 4, OutCap: 64}, nil, nil, parseOrder)

	root.NextStage("inventory", pipeline.StageConfig{Workers: 4, OutCap: 64}, nil,
		func(ctx context.Context, o OrderInfo) (branchResult, error) {
			time.Sleep(time.Duration(8000+rand.Intn(12000)) * time.Microsecond) // 8-20ms
			if rand.Intn(100) < 3 {
				return branchResult{}, fmt.Errorf("库存不足: %s", o.Product)
			}
			return branchResult{Branch: "inventory", OrderID: o.ID, Status: "ok",
				Detail: fmt.Sprintf("%s 充足", o.Product)}, nil
		}).Sink(func(ctx context.Context, r branchResult) {})

	root.NextStage("payment", pipeline.StageConfig{Workers: 4, OutCap: 64}, nil,
		func(ctx context.Context, o OrderInfo) (branchResult, error) {
			time.Sleep(time.Duration(15000+rand.Intn(25000)) * time.Microsecond) // 15-40ms
			if rand.Intn(100) < 5 {
				return branchResult{}, fmt.Errorf("支付失败: 余额不足")
			}
			return branchResult{Branch: "payment", OrderID: o.ID, Status: "ok",
				Detail: fmt.Sprintf("扣款 %.2f", o.Amount)}, nil
		}).Sink(func(ctx context.Context, r branchResult) {})

	root.NextStage("risk", pipeline.StageConfig{Workers: 4, OutCap: 64}, nil,
		func(ctx context.Context, o OrderInfo) (branchResult, error) {
			time.Sleep(time.Duration(25000+rand.Intn(35000)) * time.Microsecond) // 25-60ms
			if rand.Intn(100) < 2 {
				return branchResult{}, fmt.Errorf("风控拒绝: 大额订单")
			}
			level := "低"
			if o.Amount > 5000 {
				level = "高"
			}
			return branchResult{Branch: "risk", OrderID: o.ID, Status: "ok",
				Detail: fmt.Sprintf("风险 %s", level)}, nil
		}).Sink(func(ctx context.Context, r branchResult) {})

	pl := pipeline.New[string, OrderInfo](pipeline.PipelineConfig{Name: "order-pipeline"}).
		AddStage(root).
		Input(&randSource{ratePerSec: 300})

	exporter := promexporter.New(pl.MetricsMonitor())
	prometheus.MustRegister(exporter)

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		fmt.Println("Metrics: http://0.0.0.0:2112/metrics")
		_ = http.ListenAndServe(":2112", nil)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Println("Pipeline: parse → {inventory, payment, risk} 每秒 ~300 条")
	fmt.Println("Grafana: http://localhost:3000  Prometheus: http://localhost:9090")
	_ = pl.Run(ctx)
	_ = pl.Close(5 * time.Second)
}