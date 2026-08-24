// Package prometheus 将 Pipeline 的实时指标暴露为 Prometheus 格式（D-37）。
//
// 零侵入主库：基于 `Monitor.Metrics()` 快照轮询，不修改主库任何代码。
// 使用方式：
//
//	import pipeline "github.com/YUJIAJING0408/pipeline"
//	import promexporter "github.com/YUJIAJING0408/pipeline/metrics/prometheus"
//
//	exporter := promexporter.New(pl.MetricsMonitor())
//	prometheus.MustRegister(exporter)
//	http.Handle("/metrics", promhttp.Handler())
package prometheus

import (
	"fmt"
	"strings"
	"sync"
	"time"

	pipeline "github.com/YUJIAJING0408/pipeline"
	"github.com/prometheus/client_golang/prometheus"
)

// metricDesc 批量创建具有相同 label 集的指标描述符。
type metricDesc struct {
	fqName string
	help   string
	labels []string
}

func (d *metricDesc) desc() *prometheus.Desc {
	return prometheus.NewDesc(d.fqName, d.help, d.labels, nil)
}

// errorCodeLabels 返回 ErrorCode 对应的 label 值数组。
// StageMonitor.ErrCodes 下标：[0]=Unknown [1]=Timeout [2]=InvalidInput [3]=Processing [4]=System
var errorCodeLabels = []string{"unknown", "timeout", "invalid_input", "processing", "system"}

// Exporter 实现 prometheus.Collector，将 Pipeline 运行时指标暴露为 Prometheus 格式。
type Exporter struct {
	mon *pipeline.Monitor

	mu        sync.Mutex
	prevTotal map[string]uint64
	prevTime  time.Time

	// 预注册的指标描述符。
	processedTotal *prometheus.Desc
	errorsTotal    *prometheus.Desc
	throughput     *prometheus.Desc
	latencyAvg     *prometheus.Desc
	latencyP50     *prometheus.Desc
	latencyP99     *prometheus.Desc
	latencyMax     *prometheus.Desc
	queueDepth     *prometheus.Desc
	blockedTotal   *prometheus.Desc
	routeAccepted  *prometheus.Desc
	routeRejected  *prometheus.Desc
}

// New 创建 Exporter。
func New(mon *pipeline.Monitor) *Exporter {
	return &Exporter{
		mon:       mon,
		prevTotal: make(map[string]uint64),
		prevTime:  time.Now(),

		processedTotal: prometheus.NewDesc("pipeline_stage_processed_total",
			"Total items processed by stage.", []string{"stage"}, nil),
		errorsTotal: prometheus.NewDesc("pipeline_stage_errors_total",
			"Total errors by stage and error code.", []string{"stage", "code"}, nil),
		throughput: prometheus.NewDesc("pipeline_stage_throughput",
			"Items per second (relative to last poll).", []string{"stage"}, nil),
		latencyAvg: prometheus.NewDesc("pipeline_stage_latency_avg_ns",
			"Average processing latency per item in nanoseconds.", []string{"stage"}, nil),
		latencyP50: prometheus.NewDesc("pipeline_stage_latency_p50_ns",
			"P50 processing latency in nanoseconds.", []string{"stage"}, nil),
		latencyP99: prometheus.NewDesc("pipeline_stage_latency_p99_ns",
			"P99 processing latency in nanoseconds.", []string{"stage"}, nil),
		latencyMax: prometheus.NewDesc("pipeline_stage_latency_max_ns",
			"Maximum processing latency in nanoseconds.", []string{"stage"}, nil),
		queueDepth: prometheus.NewDesc("pipeline_stage_queue_depth",
			"Current number of items waiting in the input queue.", []string{"stage"}, nil),
		blockedTotal: prometheus.NewDesc("pipeline_stage_blocked_ns_total",
			"Total nanoseconds spent blocked on output writes.", []string{"stage"}, nil),
		routeAccepted: prometheus.NewDesc("pipeline_stage_route_accepted_total",
			"Total items accepted by at least one route branch.", []string{"stage"}, nil),
		routeRejected: prometheus.NewDesc("pipeline_stage_route_rejected_total",
			"Total items rejected by all route branches.", []string{"stage"}, nil),
	}
}

// Describe 实现 prometheus.Collector。
func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.processedTotal
	ch <- e.errorsTotal
	ch <- e.throughput
	ch <- e.latencyAvg
	ch <- e.latencyP50
	ch <- e.latencyP99
	ch <- e.latencyMax
	ch <- e.queueDepth
	ch <- e.blockedTotal
	ch <- e.routeAccepted
	ch <- e.routeRejected
}

// Collect 实现 prometheus.Collector：轮询 Monitor.Metrics() 快照并推送指标。
func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	metrics := e.mon.Metrics()
	now := time.Now()

	e.mu.Lock()
	elapsed := now.Sub(e.prevTime)
	if e.prevTime.IsZero() {
		elapsed = time.Second
		e.prevTime = now
	}
	if e.prevTotal == nil {
		e.prevTotal = make(map[string]uint64)
	}

	for _, m := range metrics {
		stage := m.StageName

		// 累计值（Counter）：total, errors, blocked, route
		ch <- prometheus.MustNewConstMetric(e.processedTotal, prometheus.CounterValue, float64(m.Total), stage)
		ch <- prometheus.MustNewConstMetric(e.blockedTotal, prometheus.CounterValue, float64(m.BlockedTime.Nanoseconds()), stage)
		ch <- prometheus.MustNewConstMetric(e.routeAccepted, prometheus.CounterValue, float64(m.RouteAccepted), stage)
		ch <- prometheus.MustNewConstMetric(e.routeRejected, prometheus.CounterValue, float64(m.RouteRejected), stage)

		// 错误分类：每个 code 一个 Counter。
		for i, cnt := range m.ErrCodes {
			if cnt > 0 {
				ch <- prometheus.MustNewConstMetric(e.errorsTotal, prometheus.CounterValue, float64(cnt), stage, errorCodeLabels[i])
			}
		}

		// 当前值（Gauge）：latency, queueDepth
		ch <- prometheus.MustNewConstMetric(e.latencyAvg, prometheus.GaugeValue, float64(m.AvgLatency.Nanoseconds()), stage)
		ch <- prometheus.MustNewConstMetric(e.latencyP50, prometheus.GaugeValue, float64(m.P50.Nanoseconds()), stage)
		ch <- prometheus.MustNewConstMetric(e.latencyP99, prometheus.GaugeValue, float64(m.P99.Nanoseconds()), stage)
		ch <- prometheus.MustNewConstMetric(e.latencyMax, prometheus.GaugeValue, float64(m.MaxLatency.Nanoseconds()), stage)
		ch <- prometheus.MustNewConstMetric(e.queueDepth, prometheus.GaugeValue, float64(m.QueueDepth), stage)

		// 吞吐量（Gauge）：相对上一帧的差值。
		prev, ok := e.prevTotal[stage]
		throughput := 0.0
		if ok && elapsed > 0 {
			throughput = float64(m.Total-prev) / elapsed.Seconds()
			if throughput < 0 {
				throughput = 0
			}
		}
		ch <- prometheus.MustNewConstMetric(e.throughput, prometheus.GaugeValue, throughput, stage)
		e.prevTotal[stage] = m.Total
	}
	e.prevTime = now
	e.mu.Unlock()
}

// MustNewConstMetric 是 prometheus.MustNewConstMetric 的便捷别名。
var MustNewConstMetric = prometheus.MustNewConstMetric

// 确保 Exporter 实现 prometheus.Collector。
var _ prometheus.Collector = (*Exporter)(nil)

// 避免未使用导入的编译错误。
var _ = fmt.Sprintf
var _ = strings.TrimSpace
var _ = sync.Mutex{}
var _ = time.Second