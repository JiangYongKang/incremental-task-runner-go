package taskrunner

import (
	"context"
	"time"
)

// Status 是任务在一次运行结论中的状态。
type Status string

const (
	// StatusRunning 任务仍在执行（通常只在执行过程中的快照里出现）。
	StatusRunning Status = "running"
	// StatusSuccess 任务真正执行并成功。
	StatusSuccess Status = "success"
	// StatusCached 任务命中有效缓存，未真正执行；对外结论与 StatusSuccess 一致。
	StatusCached Status = "cached"
	// StatusFailed 任务执行返回了普通错误。
	StatusFailed Status = "failed"
	// StatusTimeout 任务执行超时。
	StatusTimeout Status = "timeout"
	// StatusCanceled 任务因取消信号或上游失败而未执行/被中断。
	StatusCanceled Status = "canceled"
)

// Terminal 报告该状态是否为终态。
func (s Status) Terminal() bool {
	switch s {
	case StatusSuccess, StatusCached, StatusFailed, StatusTimeout, StatusCanceled:
		return true
	default:
		return false
	}
}

// OK 表示该状态是否代表成功结论（命中缓存与真实成功等价）。
func (s Status) OK() bool { return s == StatusSuccess || s == StatusCached }

// TaskFunc 是任务执行函数。
//
// depResults 只包含直接依赖任务的成功结果（含缓存命中结果）；
// 若某个依赖未成功，该任务根本不会被调度执行。
type TaskFunc func(ctx context.Context, input []byte, depResults map[string][]byte) (output []byte, err error)

// Task 是一个任务声明。
type Task struct {
	// Name 任务唯一标识，不能为空。
	Name string
	// Deps 直接依赖的任务名；必须全部已声明且不能成环。
	Deps []string
	// Input 任务输入，参与结果标识计算；输入变化缓存立即失效。
	Input []byte
	// Timeout 单个任务的执行超时；零值表示不单独限制（仍受 Run context 约束）。
	Timeout time.Duration
	// Fn 真正的执行逻辑；命中缓存时不会被调用。
	Fn TaskFunc
}

// Result 是单个任务的结论。
type Result struct {
	// Name 任务名。
	Name string
	// Status 终态状态。
	Status Status
	// Output 执行输出或缓存输出；失败/取消时可能为 nil。
	Output []byte
	// Err 失败、超时、取消、旧缓存等情况下的原因，可用 errors.Is 区分。
	Err error
	// Key 本任务按当前输入与依赖结果计算出的结果标识。
	Key string
	// Deps 实际参与 Key 计算的依赖结果标识（依赖名 -> 依赖结果 Key）。
	Deps map[string]string
	// Dur 真实执行耗时；命中缓存或未执行时为 0。
	Dur time.Duration
}

// Report 是一次 Run 的完整结论。
type Report struct {
	// Results 按任务名索引的结论。
	Results map[string]*Result
}

// Get 返回指定任务的结论。
func (r *Report) Get(name string) *Result { return r.Results[name] }

// Failed 返回所有非成功终态任务的结论。
func (r *Report) Failed() []*Result {
	out := make([]*Result, 0)
	for _, res := range r.Results {
		if !res.Status.OK() {
			out = append(out, res)
		}
	}
	return out
}

// Options 控制 Runner 的调度行为。
type Options struct {
	// MaxConcurrency 同一时刻真实执行任务的并发上限；<=0 时取 1。
	MaxConcurrency int
}

// CacheStore 是结果缓存存储抽象。
//
// Get 按 key 取缓存条目；返回值 found=false 表示未命中。
// Put 写入/覆盖条目。删除由 Runner 负责（invalidateOnMiss）。
type CacheStore interface {
	Get(key string) (entry CacheEntry, found bool)
	Put(key string, entry CacheEntry)
	Delete(key string)
}

// CacheEntry 是一条缓存记录。
type CacheEntry struct {
	// Key 写入该条目时使用的结果标识。
	Key string
	// Output 任务输出。
	Output []byte
}
