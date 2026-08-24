package pipeline

import (
	"fmt"
)

// topoNode 供拓扑校验器统一访问 Stage 与 MergeNode 的拓扑结构（D-28）。
// 内部接口（小写），不参与对外 API。
type topoNode interface {
	topoName() string
	// topoDataOut 返回数据流出边：Stage 为 NextStage 子节点；MergeNode 为有 ch 的下游。
	topoDataOut() []Stager
	// topoLifeOut 返回生命周期出边：递归 Start/Close 遍历的子节点。
	topoLifeOut() []Stager
	// topoBranches 返回 MergeNode 的 Wire 分支（Stage 返回 nil）。
	topoBranches() []Stager
	// topoBranchReqs 返回 MergeNode 的分支数要求（Stage 返回 0）。
	topoBranchReqs() int
}

var _ topoNode = (*Stage[int, int])(nil)

// mergeMarker 标记 MergeNode 类型（D-28，泛型无法用具体类型断言，改用 marker 接口）。
type mergeMarker interface{ isMergeNode() }

func (j *MergeNode[T]) isMergeNode() {}

func (s *Stage[T1, T2]) topoName() string       { return s.name }
func (s *Stage[T1, T2]) topoDataOut() []Stager  { return s.dataSubs }
func (s *Stage[T1, T2]) topoLifeOut() []Stager  { return s.subStages }
func (s *Stage[T1, T2]) topoBranches() []Stager { return nil }
func (s *Stage[T1, T2]) topoBranchReqs() int    { return 0 }

func (j *MergeNode[T]) topoName() string { return j.name }
func (j *MergeNode[T]) topoDataOut() []Stager {
	var out []Stager
	for _, o := range j.outs {
		if o.ch != nil {
			out = append(out, o.stage)
		}
	}
	return out
}
func (j *MergeNode[T]) topoLifeOut() []Stager { return j.subStages }
func (j *MergeNode[T]) topoBranches() []Stager {
	out := make([]Stager, 0, len(j.srcs))
	for _, s := range j.srcs {
		out = append(out, s.stage)
	}
	return out
}
func (j *MergeNode[T]) topoBranchReqs() int { return j.cfg.Size }

// 三色标记常量（DFS 环检测）。
const (
	topoWhite = 0 // 未访问
	topoGray  = 1 // 访问中（在调用栈上）
	topoBlack = 2 // 已完
)

// validator 执行拓扑校验（D-28）。
type validator struct {
	root   Stager // 根 Stage
	state  map[Stager]int // 三色标记
	errs   []error
	path   []string // 当前 DFS 路径（环报告）
	seen   map[Stager]bool // 已发现节点
}

// Validate 校验 Pipeline 拓扑，返回全部错误（一次性暴露，不逐个卡）。
//
// 检测项（D-28）：
//   - 数据流无环（DFS 三色标记）
//   - MergeNode 分支数匹配（len(Wire) == JoinConfig.Size）
//   - MergeNode 生命周期已挂载（每个 Wire 的分支必须 Attach 了该 merge）
//   - 分支 / 下游不重复
//   - 无孤立节点（被引用但不在 root 数据流图中）
func (p *Pipeline[T1, T2]) Validate() []error {
	if p.stage == nil {
		return []error{ErrPipelineEmpty}
	}
	v := &validator{root: p.stage, state: make(map[Stager]int), seen: make(map[Stager]bool)}

	// 1. 收集所有节点（以 root 为根，沿生命周期边 BFS）。
	v.collect()

	// 2. 环检测：从 root 沿数据流出边 DFS。
	v.detectCycle(v.root)

	// 3. 对每个 MergeNode 做完整性校验。
	for node := range v.seen {
		if isMergeNode(node) {
			v.checkMerge(node)
		}
	}

	// 4. 孤立节点：生命周期可达但不在数据流可达集。
	v.checkOrphans()

	return v.errs
}

// isMergeNode 判断节点是否为 MergeNode（泛型类型无法用具体类型断言，用 marker 接口）。
func isMergeNode(node Stager) bool {
	_, ok := node.(mergeMarker)
	return ok
}

// dataOutOf 返回节点的数据流出边集（D-28，拓扑校验的数据流图模型）：
//
//	Stage:     NextStage 子节点（dataSubs）+ subStages 中 Attach 的 MergeNode（分支→merge）
//	MergeNode: outs 中有 ch 的下游
func dataOutOf(node Stager) []Stager {
	tn, ok := node.(topoNode)
	if !ok {
		return nil
	}
	var out []Stager
	out = append(out, tn.topoDataOut()...)
	// 非 MergeNode：其生命周期子节点中若含 MergeNode，则该 merge 是本节点的数据流下游
	// （因为 merge.Wire 引用本节点，本节点的输出被 merge 消费）。
	if !isMergeNode(node) {
		for _, st := range tn.topoLifeOut() {
			if isMergeNode(st) {
				out = append(out, st)
			}
		}
	}
	return out
}

// collect BFS 收集所有节点（生命周期边：subStages，含 Attach 的 merge）。
func (v *validator) collect() {
	queue := []Stager{v.root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == nil || v.seen[cur] {
			continue
		}
		v.seen[cur] = true
		tn, ok := cur.(topoNode)
		if !ok {
			continue
		}
		// 生命周期出边（含 NextStage + Attach）。
		queue = append(queue, tn.topoLifeOut()...)
	}
}

// detectCycle 从 root 沿数据流边 DFS，遇灰节点报环。
func (v *validator) detectCycle(node Stager) {
	tn, ok := node.(topoNode)
	if !ok {
		return
	}
	v.state[node] = topoGray
	v.path = append(v.path, tn.topoName())
	for _, child := range dataOutOf(node) {
		switch v.state[child] {
		case topoGray:
			v.errs = append(v.errs, fmt.Errorf("cycle detected: %s",
				formatPath(append(v.path, childName(child)))))
		case topoWhite:
			v.detectCycle(child)
		}
	}
	v.path = v.path[:len(v.path)-1]
	v.state[node] = topoBlack
}

// checkMerge 校验单个 MergeNode：分支数 / Attach / 重复。
func (v *validator) checkMerge(node Stager) {
	tn := node.(topoNode)
	name := tn.topoName()

	// 分支数匹配。
	branches := tn.topoBranches()
	if len(branches) != tn.topoBranchReqs() {
		v.errs = append(v.errs, fmt.Errorf("merge '%s': wired=%d size=%d",
			name, len(branches), tn.topoBranchReqs()))
	}

	// 每个分支必须 Attach 了该 merge（生命周期挂载）。
	for _, b := range branches {
		bn, ok := b.(topoNode)
		if !ok {
			continue
		}
		attached := false
		for _, st := range bn.topoLifeOut() {
			if st == node {
				attached = true
				break
			}
		}
		if !attached {
			v.errs = append(v.errs, fmt.Errorf("merge '%s' not attached to branch '%s'",
				name, childName(b)))
		}
	}

	// 下游不重复。
	seenOut := make(map[Stager]bool)
	for _, o := range tn.topoDataOut() {
		if seenOut[o] {
			v.errs = append(v.errs, fmt.Errorf("merge '%s': duplicate downstream '%s'",
				name, childName(o)))
		}
		seenOut[o] = true
	}
}

// checkOrphans 检测被引用但不在数据流图中的节点。
func (v *validator) checkOrphans() {
	// 数据流可达集：从 root 沿数据流出边（dataOutOf）BFS。
	dataReach := make(map[Stager]bool)
	queue := []Stager{v.root}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == nil || dataReach[cur] {
			continue
		}
		dataReach[cur] = true
		queue = append(queue, dataOutOf(cur)...)
	}
	// 生命周期可达集 vs 数据流可达集。
	for node := range v.seen {
		if !dataReach[node] {
			v.errs = append(v.errs, fmt.Errorf("unreachable: '%s' not in data flow graph", childName(node)))
		}
	}
}

// formatPath 将路径列表格式化为 "a -> b -> c"。
func formatPath(path []string) string {
	if len(path) == 0 {
		return ""
	}
	out := path[0]
	for _, p := range path[1:] {
		out += " -> " + p
	}
	return out
}

// childName 安全获取节点名称。
func childName(st Stager) string {
	if st == nil {
		return "<nil>"
	}
	if tn, ok := st.(topoNode); ok {
		return tn.topoName()
	}
	return st.Name()
}