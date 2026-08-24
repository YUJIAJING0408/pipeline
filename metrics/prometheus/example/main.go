// Command example 演示 Prometheus 子模块的接入方式。
//
// 持续产生随机数据（每秒 ~300 条），供 Prometheus + Grafana 观察。
//
// 运行前需在子模块目录执行 go mod tidy 拉取依赖：
//
//	cd metrics/prometheus && go mod tidy && go run ./example
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pipeline "github.com/YUJIAJING0408/pipeline"
	promexporter "github.com/YUJIAJING0408/pipeline/metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// randSource 每秒产生约 300 条随机字符串，直到 ctx 取消。
type randSource struct {
	ratePerSec float64
}

func (s *randSource) Start(ctx context.Context, out chan<- string) error {
	interval := time.Duration(float64(time.Second) / s.ratePerSec)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	words := []string{"order", "payment", "refund", "query", "login", "logout", "register", "report"}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			word := words[rand.Intn(len(words))]
			val := rand.Intn(10000)
			select {
			case out <- fmt.Sprintf("%s:%d", word, val):
			case <-ctx.Done():
				return nil
			}
		}
	}
}

func main() {
	// 1. 构建 Pipeline：parse（string→int） → transform（int→int） → sink。
	s1 := pipeline.NewStage("parse", pipeline.StageConfig{Workers: 4, OutCap: 64}, nil, nil,
		func(ctx context.Context, in string) (int, error) {
			time.Sleep(time.Duration(rand.Intn(2000)) * time.Microsecond)
			return len(in), nil
		})
	s1.NextStage("transform", pipeline.StageConfig{Workers: 4, OutCap: 64}, nil,
		func(ctx context.Context, in int) (int, error) {
			time.Sleep(time.Duration(rand.Intn(3000)) * time.Microsecond)
			if rand.Intn(100) < 2 { // 2% 错误率
				return 0, fmt.Errorf("random error")
			}
			return in * 10, nil
		}).Sink(func(ctx context.Context, v int) {})

	pl := pipeline.New[string, int](pipeline.PipelineConfig{Name: "prometheus-demo"}).
		AddStage(s1).
		Input(&randSource{ratePerSec: 300})

	// 2. 创建 Prometheus Exporter 并注册。
	exporter := promexporter.New(pl.MetricsMonitor())
	prometheus.MustRegister(exporter)

	// 3. 启动 HTTP 暴露 /metrics。
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		fmt.Println("Metrics HTTP 服务已启动: http://0.0.0.0:2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatal(err)
		}
	}()

	// 4. 运行 Pipeline 直到收到退出信号。
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Println("Pipeline 已启动，每秒 ~300 条随机数据，等待 Prometheus 拉取…")
	if err := pl.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
	if err := pl.Close(5 * time.Second); err != nil {
		log.Fatalf("close: %v", err)
	}
	fmt.Println("Pipeline 已关闭")

	// 4. 运行 Pipeline。
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(1800 * time.Second)
		cancel()
	}()
	if err := pl.Run(ctx); err != nil {
		log.Fatalf("run: %v", err)
	}
	if err := pl.Close(5 * time.Second); err != nil {
		log.Fatalf("close: %v", err)
	}
	fmt.Println("Pipeline 已关闭")
}
