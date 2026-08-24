package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// keyedVal 实现 Keyable 的测试类型。
type keyedVal struct{ K string; V int }

func (k keyedVal) MergeKey() string { return k.K }

// TestValidateOK 验证合法链式图校验通过（0 错误）。
func TestValidateOK(t *testing.T) {
	root := NewStage("s1", StageConfig{Workers: 1, OutCap: 4}, nil, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	root.NextStage("s2", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })

	pl := New[int, int](PipelineConfig{Name: "ok"}).AddStage(root)
	if errs := pl.Validate(); len(errs) != 0 {
		t.Fatalf("合法图不应报错: %v", errs)
	}
}

// TestValidateNoStage 验证无 Stage 时返回 ErrPipelineEmpty。
func TestValidateNoStage(t *testing.T) {
	pl := New[int, int](PipelineConfig{Name: "empty"})
	errs := pl.Validate()
	if len(errs) != 1 || !errors.Is(errs[0], ErrPipelineEmpty) {
		t.Fatalf("expect ErrPipelineEmpty, got %v", errs)
	}
}

// TestValidateCycle 验证数据流环检测。
func TestValidateCycle(t *testing.T) {
	root := NewStage("a", StageConfig{Workers: 1, OutCap: 4}, nil, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })

	// 构造环：通过手动接线 a.dataSubs 指向自身（模拟用户错误接线）。
	root.dataSubs = append(root.dataSubs, root)

	pl := New[int, int](PipelineConfig{Name: "cycle"}).AddStage(root)
	errs := pl.Validate()
	if len(errs) == 0 {
		t.Fatal("环图应报错")
	}
	found := false
	for _, e := range errs {
		if containsErr(e, "cycle") {
			found = true
		}
	}
	if !found {
		t.Errorf("应包含 cycle 错误: %v", errs)
	}
}

// TestValidateMergeSize 验证 MergeNode 分支数不匹配。
func TestValidateMergeSize(t *testing.T) {
	root := NewStage("root", StageConfig{Workers: 1, OutCap: 4}, nil, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
	b1 := root.NextStage("b1", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
	b2 := root.NextStage("b2", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })

	merge := NewMergeNode("merge", JoinConfig[keyedVal]{Size: 3},
		func(ctx context.Context, batch []keyedVal) (keyedVal, error) { return keyedVal{}, nil })
	merge.Wire(b1).Wire(b2) // 只有 2 个分支，Size=3
	b1.Attach(merge)
	b2.Attach(merge)

	pl := New[keyedVal, keyedVal](PipelineConfig{Name: "size"}).AddStage(root)
	errs := pl.Validate()
	if len(errs) == 0 {
		t.Fatal("分支数不匹配应报错")
	}
	found := false
	for _, e := range errs {
		if containsErr(e, "wired") {
			found = true
		}
	}
	if !found {
		t.Errorf("应包含 wired/size 错误: %v", errs)
	}
}

// TestValidateMergeNoAttach 验证 Wire 了但未 Attach。
func TestValidateMergeNoAttach(t *testing.T) {
	root := NewStage("root", StageConfig{Workers: 1, OutCap: 4}, nil, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
	b1 := root.NextStage("b1", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
	b2 := root.NextStage("b2", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
	b3 := root.NextStage("b3", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })

	merge := NewMergeNode("merge", JoinConfig[keyedVal]{Size: 3},
		func(ctx context.Context, batch []keyedVal) (keyedVal, error) { return keyedVal{}, nil })
	merge.Wire(b1).Wire(b2).Wire(b3)
	b1.Attach(merge)
	b2.Attach(merge)
	// b3 忘了 Attach。

	pl := New[keyedVal, keyedVal](PipelineConfig{Name: "noattach"}).AddStage(root)
	errs := pl.Validate()
	if len(errs) == 0 {
		t.Fatal("缺 Attach 应报错")
	}
	found := false
	for _, e := range errs {
		if containsErr(e, "not attached") {
			found = true
		}
	}
	if !found {
		t.Errorf("应包含 not attached 错误: %v", errs)
	}
}

// TestValidateOrphan 验证孤立节点检测。
func TestValidateOrphan(t *testing.T) {
	root := NewStage("root", StageConfig{Workers: 1, OutCap: 4}, nil, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	root.NextStage("s1", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })

	// 孤立节点：被 Attach 但不在数据流中。
	ghost := NewStage("ghost", StageConfig{Workers: 1, OutCap: 4}, nil, nil,
		func(ctx context.Context, x int) (int, error) { return x, nil })
	// 手动把一个 ghost 挂到 root 的生命周期但不接入数据流（只进 subStages，不进 dataSubs）。
	root.subStages = append(root.subStages, ghost)

	pl := New[int, int](PipelineConfig{Name: "orphan"}).AddStage(root)
	errs := pl.Validate()
	if len(errs) == 0 {
		t.Fatal("孤立节点应报错")
	}
}

// TestValidateMultiError 验证一次性返回多个错误。
func TestValidateMultiError(t *testing.T) {
	root := NewStage("root", StageConfig{Workers: 1, OutCap: 4}, nil, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
	b1 := root.NextStage("b1", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
	b2 := root.NextStage("b2", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
	b3 := root.NextStage("b3", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })

	// 两个 merge 各自都有问题：merge1 Wire 数不足 + merge2 缺 Attach。
	m1 := NewMergeNode("m1", JoinConfig[keyedVal]{Size: 3},
		func(ctx context.Context, batch []keyedVal) (keyedVal, error) { return keyedVal{}, nil })
	m1.Wire(b1).Wire(b2) // 缺 b3
	b1.Attach(m1)

	m2 := NewMergeNode("m2", JoinConfig[keyedVal]{Size: 3},
		func(ctx context.Context, batch []keyedVal) (keyedVal, error) { return keyedVal{}, nil })
	m2.Wire(b1).Wire(b2).Wire(b3)
	b1.Attach(m2)
	b2.Attach(m2)
	b3.Attach(m2)

	pl := New[keyedVal, keyedVal](PipelineConfig{Name: "multi"}).AddStage(root)
	errs := pl.Validate()
	if len(errs) < 2 {
		t.Fatalf("应有多于 1 个错误, got %d: %v", len(errs), errs)
	}
}

/*
验证 MergeNode 正常（Wire 数=Size + 全部 Attach）不报错。
*/
func TestValidateMergeOK(t *testing.T) {
	root := NewStage("root", StageConfig{Workers: 1, OutCap: 4}, nil, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
	var branches []*Stage[keyedVal, keyedVal]
	for i := 0; i < 3; i++ {
		b := root.NextStage("b", StageConfig{Workers: 1, OutCap: 4}, nil,
			func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })
		branches = append(branches, b)
	}
	merge := NewMergeNode("merge", JoinConfig[keyedVal]{Size: 3},
		func(ctx context.Context, batch []keyedVal) (keyedVal, error) { return keyedVal{}, nil })
	for _, b := range branches {
		merge.Wire(b)
		b.Attach(merge)
	}
	merge.NextStage("sink", StageConfig{Workers: 1, OutCap: 4}, nil,
		func(ctx context.Context, x keyedVal) (keyedVal, error) { return x, nil })

	pl := New[keyedVal, keyedVal](PipelineConfig{Name: "mergeok"}).AddStage(root)
	if errs := pl.Validate(); len(errs) != 0 {
		t.Fatalf("合法的 MergeNode 图不应报错: %v", errs)
	}
}

// containsErr 判断错误消息是否包含子串。
func containsErr(e error, substr string) bool {
	if e == nil {
		return false
	}
	return strings.Contains(e.Error(), substr)
}