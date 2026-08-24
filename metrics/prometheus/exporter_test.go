package prometheus

import (
	"context"
	"testing"
	"time"

	pipeline "github.com/YUJIAJING0408/pipeline"
	"github.com/prometheus/client_golang/prometheus"
)

// TestExporterCollect 验证 Exporter 通过实际 Pipeline 运行采集指标。
func TestExporterCollect(t *testing.T) {
	rootIn := make(chan int, 16)
	root := pipeline.NewStage("root", pipeline.StageConfig{Workers: 2, OutCap: 8}, rootIn, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	pl := pipeline.New[int, int](pipeline.PipelineConfig{Name: "exporter_test"}).AddStage(root)
	mon := pl.MetricsMonitor()
	exporter := New(mon)
	reg := prometheus.NewPedanticRegistry()
	if err := reg.Register(exporter); err != nil {
		t.Fatalf("注册 Exporter 失败: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := root.Start(ctx, nil); err != nil {
		t.Fatal(err)
	}
	rootIn <- 1; rootIn <- 2; rootIn <- 3
	close(rootIn)
	root.pool.Wait()
	go func() { for range root.OutChan() {} }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	_ = root.Close(time.Second)
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather 失败: %v", err)
	}
	if len(families) == 0 {
		t.Fatal("Gather 返回空指标集")
	}
}

// TestExporterMetricNames 验证指标名称完整。
func TestExporterMetricNames(t *testing.T) {
	rootIn := make(chan int, 8)
	root := pipeline.NewStage("root", pipeline.StageConfig{Workers: 1, OutCap: 8}, rootIn, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	pl := pipeline.New[int, int](pipeline.PipelineConfig{Name: "names"}).AddStage(root)
	mon := pl.MetricsMonitor()
	exporter := New(mon)
	reg := prometheus.NewPedanticRegistry()
	_ = reg.Register(exporter)
	ctx, cancel := context.WithCancel(context.Background())
	_ = root.Start(ctx, nil)
	rootIn <- 1
	close(rootIn)
	root.pool.Wait()
	go func() { for range root.OutChan() {} }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	_ = root.Close(time.Second)
	families, _ := reg.Gather()
	names := make(map[string]bool)
	for _, f := range families {
		names[f.GetName()] = true
	}
	expected := []string{
		"pipeline_stage_processed_total",
		"pipeline_stage_latency_avg_ns",
		"pipeline_stage_latency_p50_ns",
		"pipeline_stage_latency_p99_ns",
		"pipeline_stage_latency_max_ns",
		"pipeline_stage_queue_depth",
		"pipeline_stage_blocked_ns_total",
		"pipeline_stage_route_accepted_total",
		"pipeline_stage_route_rejected_total",
		"pipeline_stage_throughput",
	}
	for _, name := range expected {
		if !names[name] {
			t.Errorf("缺少指标: %s", name)
		}
	}
}

var _ = time.Second