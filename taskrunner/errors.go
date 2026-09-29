// Package taskrunner 提供本地增量任务执行与结果缓存能力。
//
// 任务以名称声明依赖，调度器按依赖层次并发执行（带全局并发上限），
// 并按「任务输入 + 依赖结果」计算结果标识做缓存；取消、超时、失败、
// 成环、重复声明等情况均以可区分的错误/状态暴露给调用方。
package taskrunner

import "fmt"

// 声明阶段的哨兵错误。
var (
	// ErrDuplicateTask 表示同名任务被重复声明。
	ErrDuplicateTask = fmt.Errorf("taskrunner: duplicate task declaration")
	// ErrDependencyMissing 表示任务依赖了一个未声明的任务。
	ErrDependencyMissing = fmt.Errorf("taskrunner: dependency not declared")
	// ErrDependencyCycle 表示任务依赖图中存在环。
	ErrDependencyCycle = fmt.Errorf("taskrunner: dependency cycle detected")
	// ErrEmptyName 表示任务名称为空。
	ErrEmptyName = fmt.Errorf("taskrunner: task name must not be empty")
)

// 运行阶段的哨兵错误。
var (
	// ErrTimeout 表示任务执行超过允许的时间。
	ErrTimeout = fmt.Errorf("taskrunner: task timeout")
	// ErrCanceled 表示运行被取消（含上游失败导致的级联取消）。
	ErrCanceled = fmt.Errorf("taskrunner: run canceled")
	// ErrDependencyFailed 表示上游任务处于非成功终态，本任务不会执行。
	ErrDependencyFailed = fmt.Errorf("taskrunner: dependency did not succeed")
	// ErrStaleCache 表示命中了旧标识的缓存：输入/依赖已变化，属于判定失败。
	ErrStaleCache = fmt.Errorf("taskrunner: stale cache entry served for changed input")
)

// DuplicateTaskError 重复声明错误，携带冲突的任务名。
type DuplicateTaskError struct {
	Name string
}

func (e *DuplicateTaskError) Error() string {
	return fmt.Sprintf("%v: %q", ErrDuplicateTask, e.Name)
}

func (e *DuplicateTaskError) Unwrap() error { return ErrDuplicateTask }

// DependencyMissingError 依赖缺失错误，携带任务名与缺失的依赖名。
type DependencyMissingError struct {
	Task       string
	Dependency string
}

func (e *DependencyMissingError) Error() string {
	return fmt.Sprintf("%v: task %q depends on %q", ErrDependencyMissing, e.Task, e.Dependency)
}

func (e *DependencyMissingError) Unwrap() error { return ErrDependencyMissing }

// DependencyCycleError 依赖成环错误，携带构成环的任务名序列。
type DependencyCycleError struct {
	Cycle []string
}

func (e *DependencyCycleError) Error() string {
	return fmt.Sprintf("%v: %v", ErrDependencyCycle, e.Cycle)
}

func (e *DependencyCycleError) Unwrap() error { return ErrDependencyCycle }

// StaleCacheError 旧缓存命中错误：输入变化却返回了旧结果。
// Key 为按当前输入计算出的结果标识，StoredKey 为缓存条目标识。
type StaleCacheError struct {
	Task      string
	Key       string
	StoredKey string
}

func (e *StaleCacheError) Error() string {
	return fmt.Sprintf("%v: task %q expected key %s but served %s",
		ErrStaleCache, e.Task, e.Key, e.StoredKey)
}

func (e *StaleCacheError) Unwrap() error { return ErrStaleCache }

// DependencyFailedError 上游未成功导致本任务跳过。
type DependencyFailedError struct {
	Task       string
	Dependency string
	Status     Status
}

func (e *DependencyFailedError) Error() string {
	return fmt.Sprintf("%v: task %q blocked by dependency %q (status %s)",
		ErrDependencyFailed, e.Task, e.Dependency, e.Status)
}

func (e *DependencyFailedError) Unwrap() error { return ErrDependencyFailed }
