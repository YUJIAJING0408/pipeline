package pipeline

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

// StageMonitor 是每个 Stage 内部的耗时与计数统计（D-02）。
//
// 由 workerPool.handle 在每次 process 后记录耗时与成败；
// Close 完成后由 Monitor 汇总所有 Stage 统计。
type StageMonitor struct {
	mu sync.Mutex

	total        uint64        // 已处理消息总计
	errors       uint64        // 处理失败消息数
	totalLatency time.Duration // 累计处理耗时（用于计算平均）
	maxLatency   time.Duration // 单条最大耗时（瓶颈定位）
	lastLatency  time.Duration // 最近一次耗时

	// 背压相关指标（D-27）。
	blockedTime time.Duration // 累计 output 写阻塞耗时（高值 = 下游瓶颈）
	depthFn     func() int    // 读取输入队列深度（Start 时设置，nil 时返回 0）

	// samples 环形缓冲保留最近 sampleBufSize 条耗时，用于计算分位数（P50/P99）。
	samples      [sampleBufSize]time.Duration
	sampleCount  int
	samplePos    int
}

// sampleBufSize 为 StageMonitor 保留的耗时样本数（滑动窗口，覆盖后丢弃最旧）。
const sampleBufSize = 4096

// record 记录一次消息处理完成后的耗时统计；failed 表示该条是否处理失败。
func (m *StageMonitor) record(latency time.Duration, failed bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total++
	m.totalLatency += latency
	m.lastLatency = latency
	if latency > m.maxLatency {
		m.maxLatency = latency
	}
	if failed {
		m.errors++
	}
	// 写入环形缓冲，满则覆盖最旧样本。
	m.samples[m.samplePos] = latency
	m.samplePos = (m.samplePos + 1) % sampleBufSize
	if m.sampleCount < sampleBufSize {
		m.sampleCount++
	}
}

// recordBlocked 记录一次 output 写阻塞耗时（D-27，背压检测）。
func (m *StageMonitor) recordBlocked(latency time.Duration) {
	if latency <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blockedTime += latency
}

// queueDepth 返回当前输入队列深度（积压数据条数，0 表示无积压）。
func (m *StageMonitor) queueDepth() int {
	if m.depthFn == nil {
		return 0
	}
	return m.depthFn()
}

// snapshot 返回当前统计的快照（供 Monitor 汇总使用）。
func (m *StageMonitor) snapshot() (total, errors uint64, avgLatency, maxLatency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.total == 0 {
		return 0, m.errors, 0, m.maxLatency
	}
	return m.total, m.errors, m.totalLatency / time.Duration(m.total), m.maxLatency
}

// p50p99 返回滑动窗口内最近样本的 P50 / P99 分位数耗时（线性插值法）。
// 样本不足时返回已有样本的近似分位；无样本返回 0。
func (m *StageMonitor) p50p99() (p50, p99 time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sampleCount == 0 {
		return 0, 0
	}
	// 线性化环形缓冲：sampleCount ≤ sampleBufSize 时 samples[0:sampleCount] 即全部有效样本。
	window := make([]time.Duration, m.sampleCount)
	copy(window, m.samples[:m.sampleCount])
	sort.Slice(window, func(i, j int) bool { return window[i] < window[j] })

	// 线性插值：位置 pos=(n-1)*q，落在两样本之间时取两者平均（Prometheus 同款方法）。
	quantile := func(q float64) time.Duration {
		pos := float64(m.sampleCount-1) * q
		lo := int(math.Floor(pos))
		hi := int(math.Ceil(pos))
		if lo == hi {
			return window[lo]
		}
		return time.Duration((float64(window[lo]) + float64(window[hi])) / 2)
	}
	return quantile(0.50), quantile(0.99)
}

// Summary 描述单个 Stage 的统计汇总。
type Summary struct {
	StageName  string
	Total      uint64
	Errors     uint64
	AvgLatency time.Duration
	MaxLatency time.Duration
}

// Monitor 聚合各 Stage 统计，输出链路时间分析报告。
//
// Pipeline.Run 时通过 params 传给每个 Stage 创建 StageMonitor 并注册；
// Pipeline.Close 完成后调用 GenerateSummary 输出汇总。
type Monitor struct {
	mu     sync.Mutex
	stages map[string]*StageMonitor // key: Stage.Name()
	order  []string                 // 记录 Stage 注册顺序，保证报告有序
}

// NewMonitor 创建一个空的 Monitor。
func NewMonitor() *Monitor {
	return &Monitor{stages: make(map[string]*StageMonitor)}
}

// Register 注册一个 Stage 的监控器（Pipeline 构建链路时调用）。
func (m *Monitor) Register(name string, sm *StageMonitor) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.stages[name]; !ok {
		m.order = append(m.order, name)
	}
	m.stages[name] = sm
}

// GenerateSummary 汇总所有 Stage 统计，按注册顺序返回。
func (m *Monitor) GenerateSummary() []Summary {
	m.mu.Lock()
	defer m.mu.Unlock()

	summary := make([]Summary, 0, len(m.order))
	for _, name := range m.order {
		sm := m.stages[name]
		if sm == nil {
			continue
		}
		total, errs, avg, max := sm.snapshot()
		summary = append(summary, Summary{
			StageName:  name,
			Total:      total,
			Errors:     errs,
			AvgLatency: avg,
			MaxLatency: max,
		})
	}
	return summary
}

// Format 返回格式化后的链路时间分析报告字符串。
func (m *Monitor) Format() string {
	summaries := m.GenerateSummary()
	var b strings.Builder
	b.WriteString("── 链路时间分析报告 ──\n")
	for _, s := range summaries {
		fmt.Fprintf(&b, "  %s: total=%d errors=%d avg=%v max=%v\n",
			s.StageName, s.Total, s.Errors, s.AvgLatency, s.MaxLatency)
	}
	return b.String()
}

// StageMetrics 描述单个 Stage 的实时运行指标（供仪表盘/监控轮询）。
type StageMetrics struct {
	StageName   string
	Total       uint64
	Errors      uint64
	AvgLatency  time.Duration
	MaxLatency  time.Duration
	P50         time.Duration
	P99         time.Duration
	QueueDepth  int           // 当前输入队列积压条数（D-27，背压检测）
	BlockedTime time.Duration // 累计 output 写阻塞耗时（D-27，高值 = 下游瓶颈）
}

// Metrics 聚合所有 Stage 的实时指标（含 P50/P99 分位数+背压指标），按注册顺序返回。
// 供实时监控面板轮询使用；与 GenerateSummary 不同，Metrics 不持有全局锁计算分位。
func (m *Monitor) Metrics() []StageMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]StageMetrics, 0, len(m.order))
	for _, name := range m.order {
		sm := m.stages[name]
		if sm == nil {
			continue
		}
		total, errs, avg, max := sm.snapshot()
		p50, p99 := sm.p50p99()
		blocked := sm.blockedTimeSnapshot()
		out = append(out, StageMetrics{
			StageName:   name,
			Total:       total,
			Errors:      errs,
			AvgLatency:  avg,
			MaxLatency:  max,
			P50:         p50,
			P99:         p99,
			QueueDepth:  sm.queueDepth(),
			BlockedTime: blocked,
		})
	}
	return out
}

// blockedTimeSnapshot 返回累计 output 写阻塞耗时（线程安全快照）。
func (m *StageMonitor) blockedTimeSnapshot() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.blockedTime
}
