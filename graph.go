package runner

import "sort"

// graph 是校验通过后的任务依赖图。
type graph struct {
	tasks map[TaskID]*Task
	// order 为确定性的拓扑顺序（ID 字典序优先的 Kahn 排序结果）。
	order []TaskID
	// deps[id] 为该任务去重后的直接依赖（按首次声明顺序）。
	deps map[TaskID][]TaskID
	// rdeps[id] 为依赖该任务的直接下游（字典序）。
	rdeps map[TaskID][]TaskID
}

// buildGraph 对任务声明做校验（非法声明、重复、缺失依赖、成环），并构造依赖图。
// 校验失败时返回可区分的具体错误类型，绝不静默跳过任何问题。
func buildGraph(tasks []*Task) (*graph, error) {
	g := &graph{
		tasks: make(map[TaskID]*Task),
		deps:  make(map[TaskID][]TaskID),
		rdeps: make(map[TaskID][]TaskID),
	}

	// 1. 逐条校验任务本身并检测重复声明。
	for _, t := range tasks {
		if t == nil {
			return nil, &InvalidTaskError{Reason: "task is nil"}
		}
		if t.ID == "" {
			return nil, &InvalidTaskError{Reason: "task id is empty"}
		}
		if t.Run == nil {
			return nil, &InvalidTaskError{ID: t.ID, Reason: "run function is nil"}
		}
		if _, exists := g.tasks[t.ID]; exists {
			return nil, &DuplicateTaskError{ID: t.ID}
		}
		g.tasks[t.ID] = t
	}

	// 2. 按声明顺序校验依赖存在性并对重复依赖去重。
	for _, t := range tasks {
		seen := make(map[TaskID]bool)
		for _, d := range t.Deps {
			if _, ok := g.tasks[d]; !ok {
				return nil, &MissingDependencyError{TaskID: t.ID, Missing: d}
			}
			if !seen[d] {
				seen[d] = true
				g.deps[t.ID] = append(g.deps[t.ID], d)
			}
		}
	}

	// 3. 环检测（DFS 三色法，邻接按字典序以得到稳定的环输出）。
	sortedDeps := make(map[TaskID][]TaskID, len(g.deps))
	for id := range g.tasks {
		d := append([]TaskID(nil), g.deps[id]...)
		sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
		sortedDeps[id] = d
	}
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[TaskID]int, len(g.tasks))
	var stack []TaskID
	onStack := make(map[TaskID]bool)
	var dfs func(id TaskID) error
	dfs = func(id TaskID) error {
		color[id] = gray
		onStack[id] = true
		stack = append(stack, id)
		for _, d := range sortedDeps[id] {
			if color[d] == white {
				if err := dfs(d); err != nil {
					return err
				}
			} else if color[d] == gray {
				// 从栈中 d 首次出现的位置截取到当前 id，构成环。
				start := 0
				for i, sid := range stack {
					if sid == d {
						start = i
						break
					}
				}
				cycle := append([]TaskID(nil), stack[start:]...)
				cycle = append(cycle, d)
				return &CycleError{Cycle: cycle}
			}
		}
		color[id] = black
		onStack[id] = false
		stack = stack[:len(stack)-1]
		return nil
	}
	ids := make([]TaskID, 0, len(g.tasks))
	for id := range g.tasks {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if color[id] == white {
			if err := dfs(id); err != nil {
				return nil, err
			}
		}
	}

	// 4. Kahn 拓扑排序（可用节点按 ID 字典序选取），保证调度顺序确定可复现。
	indeg := make(map[TaskID]int, len(g.tasks))
	for id := range g.tasks {
		indeg[id] = len(g.deps[id])
	}
	ready := make([]TaskID, 0)
	for id, n := range indeg {
		if n == 0 {
			ready = append(ready, id)
		}
	}
	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool { return ready[i] < ready[j] })
		id := ready[0]
		ready = ready[1:]
		g.order = append(g.order, id)
		// 收集下游：被 id 指向的任务即声明了 id 为依赖的任务。
		var downstream []TaskID
		for other, ds := range g.deps {
			for _, d := range ds {
				if d == id {
					downstream = append(downstream, other)
					break
				}
			}
		}
		sort.Slice(downstream, func(i, j int) bool { return downstream[i] < downstream[j] })
		g.rdeps[id] = append(g.rdeps[id], downstream...)
		for _, other := range downstream {
			indeg[other]--
			if indeg[other] == 0 {
				ready = append(ready, other)
			}
		}
	}
	if len(g.order) != len(g.tasks) {
		// 理论上前置 DFS 已覆盖此情况，双保险。
		return nil, &CycleError{}
	}
	return g, nil
}
