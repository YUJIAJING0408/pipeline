package pipeline

import (
	"context"
	_ "embed" // 供 //go:embed 指令使用（嵌入 index.html 字符串）
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

//go:embed index.html
var indexHTML string

// MetricsServer 提供 HTTP 实时指标面板：
//
//   - GET /          → index.html 前端页面
//   - GET /metrics   → SSE 流，每 RefreshInterval 推送一次全量 Stage 指标 JSON
//
// 指标来源为 Pipeline/Monitor，由用户创建后显式绑定。
type MetricsServer struct {
	Monitor *Monitor

	// Addr 为监听地址，如 ":8080"。
	Addr string
	// RefreshInterval 为 SSE 推送间隔，0 时默认 1s。
	RefreshInterval time.Duration

	server *http.Server

	mu        sync.Mutex
	prevTotal map[string]uint64 // 上一次各 Stage 处理总数（用于计算吞吐）
	prevTime  time.Time
}

func (ms *MetricsServer) interval() time.Duration {
	if ms.RefreshInterval <= 0 {
		return time.Second
	}
	return ms.RefreshInterval
}

// Handler 返回该服务绑定的完整 HTTP Handler（包含主页 + SSE 流），便于测试复用或挂载到用户服务器。
//
// 挂载到用户服务器示例：
//
//	mux := http.NewServeMux()
//	mux.Handle("/pipeline/", http.StripPrefix("/pipeline", ms.Handler()))
//	http.ListenAndServe(":8080", mux)
func (ms *MetricsServer) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", ms.handleIndex)
	mux.HandleFunc("/metrics", ms.handleSSE)
	return mux
}

// IndexHandler 返回仅处理主页（GET /）的 HTTP Handler，可单独挂载到用户服务器的自定义路径。
func (ms *MetricsServer) IndexHandler() http.HandlerFunc {
	return ms.handleIndex
}

// MetricsHandler 返回仅处理 SSE 流（GET /metrics）的 HTTP Handler，可单独挂载到用户服务器的自定义路径。
func (ms *MetricsServer) MetricsHandler() http.HandlerFunc {
	return ms.handleSSE
}

// Start 启动 HTTP 监听（阻塞）。调用方应另起 goroutine 运行，或使用 Shutdown 优雅停止。
func (ms *MetricsServer) Start() error {
	ms.server = &http.Server{
		Addr:    ms.Addr,
		Handler: ms.Handler(),
	}
	return ms.server.ListenAndServe()
}

// StartAsync 在后台 goroutine 启动监听并立即返回；启动失败时经 errCh 报告。
func (ms *MetricsServer) StartAsync() (<-chan error, error) {
	errCh := make(chan error, 1)
	go func() {
		errCh <- ms.Start()
	}()
	// 等待一小会儿确认端口是否真的绑定失败（如端口占用会立刻返回错误）。
	select {
	case err := <-errCh:
		return nil, err
	case <-time.After(20 * time.Millisecond):
		return errCh, nil
	}
}

// Shutdown 优雅停止 HTTP 监听。
func (ms *MetricsServer) Shutdown(ctx context.Context) error {
	if ms.server == nil {
		return nil
	}
	return ms.server.Shutdown(ctx)
}

func (ms *MetricsServer) handleIndex(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(indexHTML))
}

// handleSSE 推送指标流：立即推送一次快照，之后每个 interval 推送一次。
func (ms *MetricsServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	relayHeaders(w.Header())

	ms.sendSnapshot(w, flusher)

	ticker := time.NewTicker(ms.interval())
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			ms.sendSnapshot(w, flusher)
		}
	}
}

// relayHeaders 设置 SSE 所需响应头。
func relayHeaders(h http.Header) {
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
}

// metricsJSON 为推送到前端的单帧数据。
type metricsJSON struct {
	Timestamp int64        `json:"timestamp"` // Unix 毫秒
	Stages    []stageEntry `json:"stages"`
}

type stageEntry struct {
	Name        string  `json:"name"`
	Total       uint64  `json:"total"`
	Errors      uint64  `json:"errors"`
	Throughput  float64 `json:"throughput"` // 每秒处理条数（相对上一帧）
	AvgLatency  int64   `json:"avgLatencyNs"`
	MaxLatency  int64   `json:"maxLatencyNs"`
	P50         int64   `json:"p50Ns"`
	P99         int64   `json:"p99Ns"`
	QueueDepth  int     `json:"queueDepth"`  // 当前输入队列积压条数（D-27）
	BlockedTime int64   `json:"blockedTimeNs"` // 累计 output 写阻塞耗时（D-27）
}

// snapshot 基于 Monitor.Metrics 构建推送帧并计算各 Stage 吞吐量。
func (ms *MetricsServer) snapshot() metricsJSON {
	now := time.Now()
	metrics := ms.Monitor.Metrics()

	ms.mu.Lock()
	defer ms.mu.Unlock()
	elapsed := now.Sub(ms.prevTime)
	if ms.prevTime.IsZero() {
		elapsed = ms.interval()
		ms.prevTime = now
	}

	frame := metricsJSON{
		Timestamp: now.UnixMilli(),
		Stages:    make([]stageEntry, 0, len(metrics)),
	}
	if ms.prevTotal == nil {
		ms.prevTotal = make(map[string]uint64)
	}
	for _, m := range metrics {
		prev, ok := ms.prevTotal[m.StageName]
		throughput := 0.0
		if ok && elapsed > 0 {
			throughput = float64(m.Total-prev) / elapsed.Seconds()
			if throughput < 0 {
				throughput = 0
			}
		}
		ms.prevTotal[m.StageName] = m.Total
		frame.Stages = append(frame.Stages, stageEntry{
			Name:        m.StageName,
			Total:       m.Total,
			Errors:      m.Errors,
			Throughput:  throughput,
			AvgLatency:  m.AvgLatency.Nanoseconds(),
			MaxLatency:  m.MaxLatency.Nanoseconds(),
			P50:         m.P50.Nanoseconds(),
			P99:         m.P99.Nanoseconds(),
			QueueDepth:  m.QueueDepth,
			BlockedTime: m.BlockedTime.Nanoseconds(),
		})
	}
	ms.prevTime = now
	return frame
}

// sendSnapshot 序列化一帧指标并写入 SSE 响应。
func (ms *MetricsServer) sendSnapshot(w http.ResponseWriter, flusher http.Flusher) {
	data, err := json.Marshal(ms.snapshot())
	if err != nil {
		return
	}
	_, err = fmt.Fprintf(w, "data: %s\n\n", data)
	if err != nil {
		return
	}
	flusher.Flush()
}