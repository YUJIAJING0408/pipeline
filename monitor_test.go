package pipeline

import (
	"context"
	"testing"
	"time"
)

// TestStageMonitorRecord 验证单个 Stage 统计累计。
func TestStageMonitorRecord(t *testing.T) {
	m := &StageMonitor{}
	m.record(10*time.Millisecond, false)
	m.record(30*time.Millisecond, true)
	m.record(20*time.Millisecond, false)

	total, errors, avg, max := m.snapshot()
	if total != 3 {
		t.Errorf("total = %d, want 3", total)
	}
	if errors != 1 {
		t.Errorf("errors = %d, want 1", errors)
	}
	if avg != 20*time.Millisecond {
		t.Errorf("avg = %v, want 20ms", avg)
	}
	if max != 30*time.Millisecond {
		t.Errorf("max = %v, want 30ms", max)
	}
}

// TestStageMonitorEmpty 验证未处理消息时快照不除零。
func TestStageMonitorEmpty(t *testing.T) {
	m := &StageMonitor{}
	total, errors, avg, max := m.snapshot()
	if total != 0 || errors != 0 || avg != 0 || max != 0 {
		t.Errorf("空监控快照应为零值, got total=%d errors=%d avg=%v max=%v",
			total, errors, avg, max)
	}
}

// TestMonitorRegisterOrder 验证 Monitor 汇总按注册顺序输出。
func TestMonitorRegisterOrder(t *testing.T) {
	mon := NewMonitor()
	mon.Register("stage-b", &StageMonitor{})
	mon.Register("stage-a", &StageMonitor{})
	mon.Register("stage-c", &StageMonitor{})

	summary := mon.GenerateSummary()
	if got := len(summary); got != 3 {
		t.Fatalf("汇总数量 = %d, want 3", got)
	}
	want := []string{"stage-b", "stage-a", "stage-c"}
	for i, w := range want {
		if summary[i].StageName != w {
			t.Errorf("第 %d 项 = %q, want %q", i, summary[i].StageName, w)
		}
	}
}

// TestMonitorSummaryContent 验证汇总包含各 Stage 统计值。
func TestMonitorSummaryContent(t *testing.T) {
	mon := NewMonitor()
	sm := &StageMonitor{}
	sm.record(5*time.Millisecond, false)
	mon.Register("stage-x", sm)

	summary := mon.GenerateSummary()
	if len(summary) != 1 {
		t.Fatalf("汇总数量 = %d, want 1", len(summary))
	}
	if summary[0].Total != 1 || summary[0].AvgLatency != 5*time.Millisecond {
		t.Errorf("汇总内容不符: %+v", summary[0])
	}
}

// TestMonitorRunningRecord 验证集成：Stage 通过 params 注册 Monitor 后 handle 记录数据能汇总。
func TestMonitorRunningRecord(t *testing.T) {
	mon := NewMonitor()
	params := map[string]any{"monitor": mon}

	in := make(chan string, 4)
	s := NewStage("s", StageConfig{Workers: 1, OutCap: 4}, in, nil, func(ctx context.Context, x string) (string, error) {
		return x, nil
	})
	if err := s.Start(context.Background(), params); err != nil {
		t.Fatalf("Start 失败: %v", err)
	}

	in <- "a"
	in <- "b"
	close(in)
	_ = s.Close(time.Second)

	sm := mon.stages["s"]
	if sm == nil {
		t.Fatal("Monitor 中未注册 stage s")
	}
	total, errors, _, _ := sm.snapshot()
	if total != 2 {
		t.Errorf("total = %d, want 2", total)
	}
	if errors != 0 {
		t.Errorf("errors = %d, want 0", errors)
	}
}

// TestStageMonitorErrCodes 验证错误分类计数（D-04）。
func TestStageMonitorErrCodes(t *testing.T) {
	m := &StageMonitor{}
	m.record(10*time.Millisecond, true)
	m.recordCode(CodeTimeout)
	m.record(5*time.Millisecond, true)
	m.recordCode(CodeInvalidInput)
	m.record(3*time.Millisecond, true)
	m.recordCode(CodeTimeout)

	codes := m.codeSnapshot()
	if codes[CodeTimeout] != 2 {
		t.Errorf("Timeout 计数 = %d, want 2", codes[CodeTimeout])
	}
	if codes[CodeInvalidInput] != 1 {
		t.Errorf("InvalidInput 计数 = %d, want 1", codes[CodeInvalidInput])
	}
	// errors 总数与分类计数一致。
	total, errs, _, _ := m.snapshot()
	if total != 3 || errs != 3 {
		t.Errorf("total=%d errors=%d, want 3/3", total, errs)
	}
}
