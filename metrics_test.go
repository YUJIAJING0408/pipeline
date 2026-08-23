package pipeline

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestStageMonitorPercentile 验证环形缓冲中 P50/P99 分位计算正确（线性插值法）。
func TestStageMonitorPercentile(t *testing.T) {
	m := &StageMonitor{}
	// 写入 0..99 共 100 个耗时样本（毫秒）。
	// 线性插值：P50 = pos(99*0.5=49.5) → avg(49ms,50ms) = 49.5ms；P99 同理 = 98.5ms。
	for i := 0; i < 100; i++ {
		m.record(time.Duration(i)*time.Millisecond, false)
	}
	p50, p99 := m.p50p99()
	if p50 != 49500*time.Microsecond {
		t.Errorf("P50 = %v, want 49.5ms", p50)
	}
	if p99 != 98500*time.Microsecond {
		t.Errorf("P99 = %v, want 98.5ms", p99)
	}
}

// TestStageMonitorPercentileEmpty 验证无样本时 P50/P99 返回 0。
func TestStageMonitorPercentileEmpty(t *testing.T) {
	m := &StageMonitor{}
	p50, p99 := m.p50p99()
	if p50 != 0 || p99 != 0 {
		t.Errorf("empty monitor P50=%v P99=%v, want 0/0", p50, p99)
	}
}

// TestStageMonitorRingOverflow 验证环形缓冲满后覆盖最旧样本，只保留最近窗口。
func TestStageMonitorRingOverflow(t *testing.T) {
	m := &StageMonitor{}
	// 先写入 sampleBufSize 个 1ms，再写入 1 个 1000ms。
	// 若缓冲正确覆盖，窗口内全部为 1ms+1000ms 的最近样本，P50 应为 1ms（1000ms 只有 1 条）。
	for i := 0; i < sampleBufSize; i++ {
		m.record(time.Millisecond, false)
	}
	m.record(time.Second, false)

	if m.sampleCount != sampleBufSize {
		t.Fatalf("sampleCount = %d, want %d（环形缓冲应保持满）", m.sampleCount, sampleBufSize)
	}
	p50, _ := m.p50p99()
	if p50 != time.Millisecond {
		t.Errorf("P50 = %v, want 1ms（旧 1ms 样本应覆盖最老一条，1000ms 仅 1 条不影响 P50）", p50)
	}
}

// TestMonitorMetrics 验证 Monitor.Metrics 聚合各 Stage 实时指标。
func TestMonitorMetrics(t *testing.T) {
	mon := NewMonitor()
	sm := &StageMonitor{}
	sm.record(10*time.Millisecond, false)
	sm.record(30*time.Millisecond, true)
	mon.Register("stage-a", sm)

	metrics := mon.Metrics()
	if len(metrics) != 1 {
		t.Fatalf("len(metrics) = %d, want 1", len(metrics))
	}
	m := metrics[0]
	if m.StageName != "stage-a" {
		t.Errorf("StageName = %q, want stage-a", m.StageName)
	}
	if m.Total != 2 || m.Errors != 1 {
		t.Errorf("Total=%d Errors=%d, want 2/1", m.Total, m.Errors)
	}
	if m.AvgLatency != 20*time.Millisecond {
		t.Errorf("AvgLatency = %v, want 20ms", m.AvgLatency)
	}
	if m.MaxLatency != 30*time.Millisecond {
		t.Errorf("MaxLatency = %v, want 30ms", m.MaxLatency)
	}
}

// TestMetricsServerIndex 验证 GET / 返回 index.html。
func TestMetricsServerIndex(t *testing.T) {
	mon := NewMonitor()
	ms := &MetricsServer{Monitor: mon}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	ms.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Errorf("Content-Type = %q, want text/html", ct)
	}
	if !strings.Contains(rec.Body.String(), "Pipeline 实时指标") {
		t.Error("index.html 缺少标题内容")
	}
}

// TestMetricsServerSSEFrame 验证 /metrics 首帧 SSE 数据格式与响应头正确。
// 使用预先取消的 ctx：handler 写完首帧后 select 立即命中 ctx.Done 返回，避免挂死。
func TestMetricsServerSSEFrame(t *testing.T) {
	mon := NewMonitor()
	sm := &StageMonitor{}
	sm.record(5*time.Millisecond, false)
	mon.Register("stage-sse", sm)
	ms := &MetricsServer{Monitor: mon, RefreshInterval: time.Hour}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 预取消：首帧写完后 handler 立即退出
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	ms.Handler().ServeHTTP(rec, req)

	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "data: ") {
		t.Fatalf("SSE 首帧缺失 data: 前缀: %q", body)
	}
	jsonPart := strings.TrimPrefix(body, "data: ")
	jsonPart = strings.TrimSpace(jsonPart)

	var frame metricsJSON
	if err := json.Unmarshal([]byte(jsonPart), &frame); err != nil {
		t.Fatalf("SSE 数据无法解析为 JSON: %v\n%s", err, jsonPart)
	}
	if len(frame.Stages) != 1 {
		t.Fatalf("stages = %d, want 1", len(frame.Stages))
	}
	s := frame.Stages[0]
	if s.Name != "stage-sse" {
		t.Errorf("stage name = %q, want stage-sse", s.Name)
	}
	if s.Total != 1 || s.Errors != 0 {
		t.Errorf("Total=%d Errors=%d, want 1/0", s.Total, s.Errors)
	}
	if s.P50 != 5_000_000 || s.P99 != 5_000_000 {
		t.Errorf("P50=%d P99=%d ns, want 5ms", s.P50, s.P99)
	}
}

// TestMetricsServerThroughput 验证吞吐量相对上一帧正确计算。
func TestMetricsServerThroughput(t *testing.T) {
	mon := NewMonitor()
	sm := &StageMonitor{}
	mon.Register("stage-tp", sm)
	ms := &MetricsServer{Monitor: mon, RefreshInterval: time.Second}

	// 第一帧：此前无样本，吞吐应为 0。
	sm.record(time.Millisecond, false)
	f1 := ms.snapshot()
	if f1.Stages[0].Throughput != 0 {
		t.Fatalf("首帧吞吐 = %v, want 0", f1.Stages[0].Throughput)
	}

	// 模拟 1 秒后新增 10 条：吞吐应 ≈ 10/s。
	ms.prevTime = ms.prevTime.Add(-time.Second)
	for i := 0; i < 10; i++ {
		sm.record(time.Millisecond, false)
	}
	f2 := ms.snapshot()
	if f2.Stages[0].Throughput < 9 || f2.Stages[0].Throughput > 11 {
		t.Errorf("第二帧吞吐 = %v, want ≈10/s", f2.Stages[0].Throughput)
	}
}

// TestMetricsServerEscapesInput 验证 HTML 转义，避免恶意 Stage 名注入页面脚本。
func TestMetricsServerEscapesInput(t *testing.T) {
	mon := NewMonitor()
	sm := &StageMonitor{}
	mon.Register("<script>alert(1)</script>", sm)
	ms := &MetricsServer{Monitor: mon}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	ms.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	if strings.Contains(body, "<script>") {
		t.Error("Stage 名未转义，存在 XSS 风险")
	}
	if !strings.Contains(body, `\u003cscript\u003e`) {
		t.Error("json.Marshal 未将 `<script>` 转义为 \\u003c（默认 HTML 安全转义应生效）")
	}
}

// TestMetricsServerEndToEnd 验证真实 Pipeline 运行中指标面板能收到数据。
func TestMetricsServerEndToEnd(t *testing.T) {
	root := NewStage("producer", StageConfig{Workers: 2, OutCap: 16}, nil, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	root.NextStage("consumer", StageConfig{Workers: 2, OutCap: 16}, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })

	pl := New[int, int](PipelineConfig{Name: "e2e", InputBufferSize: 16}).
		AddStage(root).
		Input(&MockSource[int]{Data: []int{1, 2, 3, 4, 5}})

	msn := pl.MetricsMonitor()
	if msn == nil {
		t.Fatal("Pipeline.MetricsMonitor() 返回 nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_ = pl.Run(ctx)

	// Pipeline 运行期间（或关闭后）应能通过 Monitor 读到各 Stage 指标。
	metrics := msn.Metrics()
	if len(metrics) != 2 {
		t.Fatalf("Metrics count = %d, want 2", len(metrics))
	}
	names := map[string]bool{}
	for _, m := range metrics {
		names[m.StageName] = true
	}
	if !names["producer"] || !names["consumer"] {
		t.Errorf("缺少 producer/consumer 指标: %+v", names)
	}
	if err := pl.Close(time.Second); err != nil {
		t.Fatal(err)
	}
	// 关闭后总数应等于输入数据量（每个 Stage 5 条）。
	for _, m := range metrics {
		if m.Total > 5 {
			t.Errorf("%s total = %d, 不应超过输入量 5", m.StageName, m.Total)
		}
	}
}