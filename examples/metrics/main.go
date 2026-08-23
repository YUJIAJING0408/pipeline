package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/pipeline"
)

func main() {
	// 持续产生数据的 Pipeline（tick 驱动，每 3ms 一条，直到 ctx 取消）
	root := pipeline.NewStage("parse", pipeline.StageConfig{Workers: 4, OutCap: 64}, nil, nil,
		func(ctx context.Context, x int) (int, error) {
			time.Sleep(2 * time.Millisecond)
			return x, nil
		})
	leaf := root.NextStage("transform", pipeline.StageConfig{Workers: 4, OutCap: 64}, nil,
		func(ctx context.Context, x int) (int, error) {
			time.Sleep(3 * time.Millisecond)
			return x, nil
		})
	// 叶子 Stage 用 Sink 消费结果——不消费则 output 积压填满会反压阻塞整条链
	leaf.Sink(func(ctx context.Context, v int) {})

	pl := pipeline.New[int, int](pipeline.PipelineConfig{Name: "demo", InputBufferSize: 64}).
		AddStage(root).
		Input(&tickSource{interval: 3 * time.Millisecond})

	// 启动指标服务器
	ms := &pipeline.MetricsServer{Monitor: pl.MetricsMonitor(), Addr: ":18080", RefreshInterval: time.Second}
	errCh, err := ms.StartAsync()
	if err != nil {
		fmt.Fprintln(os.Stderr, "metrics server start failed:", err)
		os.Exit(1)
	}
	go func() {
		if e := <-errCh; e != nil && e != http.ErrServerClosed {
			fmt.Fprintln(os.Stderr, "metrics server error:", e)
		}
	}()

	// 终端打印访问地址，由用户自行打开浏览器查看
	fmt.Println("Pipeline 实时指标已启动:")
	fmt.Println("  本地访问:  http://127.0.0.1:18080/")
	fmt.Println("  局域网访问: http://<本机IP>:18080/")
	fmt.Println("  页面每 1 秒自动刷新（SSE 推送），Ctrl+C 退出")

	// 等待 Ctrl+C 优雅退出
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := pl.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "pipeline run error:", err)
	}
	if err := pl.Close(5 * time.Second); err != nil {
		fmt.Fprintln(os.Stderr, "pipeline close error:", err)
	}
	if err := ms.Shutdown(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "metrics server shutdown error:", err)
	}
	fmt.Println("已退出")
}

// tickSource 每 interval 产生一条递增数据，直到 ctx 取消（常驻演示用）。
type tickSource struct {
	interval time.Duration
}

func (s *tickSource) Start(ctx context.Context, out chan<- int) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	i := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			select {
			case out <- i:
				i++
			case <-ctx.Done():
				return nil
			}
		}
	}
}