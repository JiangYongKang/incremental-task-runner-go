package taskrunner

// validateAndLevel 对任务声明做静态校验，并返回按依赖层次分组的执行计划。
//
// 拒绝原因（互斥、可区分）：
//   - ErrEmptyName：任务名为空；
//   - ErrDuplicateTask：同名任务重复声明；
//   - ErrDependencyMissing：依赖了未声明的任务；
//   - ErrDependencyCycle：依赖成环（携带环上的一条任务序列）。
//
// levels[0] 为无依赖任务层，其后每层只依赖更前层的任务；同层任务可并发。
func validateAndLevel(tasks []Task) (levels [][]string, err error) {
	index := make(map[string]int, len(tasks))
	for _, t := range tasks {
		if t.Name == "" {
			return nil, ErrEmptyName
		}
		if _, dup := index[t.Name]; dup {
			return nil, &DuplicateTaskError{Name: t.Name}
		}
		index[t.Name] = 1
	}

	// 依赖去重；缺失依赖在此阶段拒绝。
	deps := make(map[string][]string, len(tasks))
	indeg := make(map[string]int, len(tasks))
	for _, t := range tasks {
		seen := make(map[string]struct{}, len(t.Deps))
		for _, d := range t.Deps {
			if _, ok := index[d]; !ok {
				return nil, &DependencyMissingError{Task: t.Name, Dependency: d}
			}
			if d == t.Name {
				// 自依赖是长度为 1 的环，交给环检测统一报告。
			}
			if _, ok := seen[d]; ok {
				continue
			}
			seen[d] = struct{}{}
			deps[t.Name] = append(deps[t.Name], d)
			indeg[t.Name]++
		}
	}

	// Kahn 分层。
	var frontier []string
	for _, t := range tasks {
		if indeg[t.Name] == 0 {
			frontier = append(frontier, t.Name)
		}
	}
	visited := 0
	for len(frontier) > 0 {
		level := append([]string(nil), frontier...)
		levels = append(levels, level)
		next := map[string]struct{}{}
		for _, name := range level {
			visited++
			for _, t := range tasks {
				if contains(deps[t.Name], name) {
					indeg[t.Name]--
					if indeg[t.Name] == 0 {
						next[t.Name] = struct{}{}
					}
				}
			}
		}
		frontier = frontier[:0]
		for n := range next {
			frontier = append(frontier, n)
		}
	}

	if visited != len(tasks) {
		return nil, &DependencyCycleError{Cycle: findCycle(deps)}
	}
	return levels, nil
}

func contains(xs []string, v string) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// findCycle 在已知存在环的依赖图中用 DFS 找出一条具体的环。
func findCycle(deps map[string][]string) []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var stack []string
	var cycle []string

	var dfs func(n string) bool
	dfs = func(n string) bool {
		color[n] = gray
		stack = append(stack, n)
		for _, d := range deps[n] {
			if color[d] == gray {
				// 从栈中 d 第一次出现的位置到栈顶构成一条环。
				start := 0
				for i, x := range stack {
					if x == d {
						start = i
						break
					}
				}
				cycle = append([]string(nil), stack[start:]...)
				cycle = append(cycle, d)
				return true
			}
			if color[d] == white && dfs(d) {
				return true
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return false
	}

	for n := range deps {
		if color[n] == white && dfs(n) {
			break
		}
	}
	return cycle
}
