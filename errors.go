package runner

import "strings"

// DuplicateTaskError 表示同一任务 ID 被重复声明。
type DuplicateTaskError struct {
	ID TaskID
}

func (e *DuplicateTaskError) Error() string {
	return "duplicate task declaration: " + string(e.ID)
}

// MissingDependencyError 表示任务声明了不存在的依赖。
type MissingDependencyError struct {
	TaskID  TaskID
	Missing TaskID
}

func (e *MissingDependencyError) Error() string {
	return "task " + string(e.TaskID) + " depends on missing task " + string(e.Missing)
}

// CycleError 表示依赖关系中存在环，Cycle 为环上的任务 ID 序列。
type CycleError struct {
	Cycle []TaskID
}

func (e *CycleError) Error() string {
	if len(e.Cycle) == 0 {
		return "dependency cycle detected"
	}
	msg := make([]string, len(e.Cycle))
	for i, id := range e.Cycle {
		msg[i] = string(id)
	}
	return "dependency cycle detected: " + strings.Join(msg, " -> ")
}

// InvalidTaskError 表示任务声明本身不合法（如 ID 为空或执行函数为空）。
type InvalidTaskError struct {
	ID     TaskID
	Reason string
}

func (e *InvalidTaskError) Error() string {
	if e.ID == "" {
		return "invalid task: " + e.Reason
	}
	return "invalid task " + string(e.ID) + ": " + e.Reason
}

// UpstreamFailureError 表示任务因依赖任务未成功而未被执行。
type UpstreamFailureError struct {
	TaskID     TaskID
	Upstream   TaskID
	UpstreamSt Status
}

func (e *UpstreamFailureError) Error() string {
	return "task " + string(e.TaskID) + " not run: upstream task " + string(e.Upstream) + " is " + e.UpstreamSt.String()
}

// CacheConsistencyError 表示缓存条目与当前输入/依赖指纹不一致却被当作命中。
type CacheConsistencyError struct {
	ID TaskID
}

func (e *CacheConsistencyError) Error() string {
	return "cache consistency violation: stale cache entry matched for task " + string(e.ID)
}
