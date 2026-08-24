// Command example 演示 Prometheus 子模块的接入方式。
//
// 运行前需在子模块目录执行 go mod tidy 拉取依赖：
//
//	cd metrics/prometheus && go mod tidy
//	go run ./example
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	pipeline "github.com/YUJIAJING0408/pipeline"
	promexporter "github.com/YUJIAJING0408/pipeline/metrics/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	// 1. 构建 Pipeline。
	s1 := pipeline.NewStage("parse", pipeline.StageConfig{Workers: 2, OutCap: 16}, nil, nil,
		func(ctx context.Context, in string) (int, error) {
			time.Sleep(2 * time.Millisecond)
			return len(in), nil
		})
	s1.NextStage("transform", pipeline.StageConfig{Workers: 2, OutCap: 16}, nil,
		func(ctx context.Context, in int) (int, error) {
			time.Sleep(3 * time.Millisecond)
			return in * 10, nil
		})

	pl := pipeline.New[string, int](pipeline.PipelineConfig{Name: "prometheus-demo"}).
		AddStage(s1).
		Input(&pipeline.MockSource[string]{Data: []string{"hello", "world", "prometheus"}})

	// 2. 创建 Prometheus Exporter 并注册。
	exporter := promexporter.New(pl.MetricsMonitor())
	prometheus.MustRegister(exporter)

	// 3. 启动 HTTP 暴露 /metrics。
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		fmt.Println("Metrics HTTP 服务已启动: http://127.0.0.1:2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatal(err)
		}
	}()

	// 4. 运行 Pipeline。
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(3 * time.Second)
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