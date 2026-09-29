package taskrunner

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Runner 持有任务声明、缓存与调度配置，可反复增量执行。
type Runner struct {
	tasks  map[string]Task
	order  []string
	levels [][]string
	store  CacheStore
	opt    Options
}

// New 构造 Runner；构造期即完成成环/缺失/重复声明校验，失败返回可区分错误。
func New(tasks []Task, store CacheStore, opt Options) (*Runner, error) {
	levels, err := validateAndLevel(tasks)
	if err != nil {
		return nil, err
	}
	if store == nil {
		store = NewMemoryStore()
	}
	if opt.MaxConcurrency <= 0 {
		opt.MaxConcurrency = 1
	}
	m := make(map[string]Task, len(tasks))
	order := make([]string, 0, len(tasks))
	for _, t := range tasks {
		m[t.Name] = t
		order = append(order, t.Name)
	}
	return &Runner{tasks: m, order: order, levels: levels, store: store, opt: opt}, nil
}

// Run 按层次调度全部任务。结论以 Report 返回；声明校验已在 New 完成，
// 因此运行期结论（失败/超时/取消）都体现在 Report 中，而非返回的 error。
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	return r.run(ctx, r.order)
}

// RunSubset 只调度 targets 及其全部（反向）依赖闭包。
//
// 未选中的任务既不会执行也不会出现在 Report 中；已完成且未失效的结果
// 仍来自缓存，因此可从上次运行的状态干净地继续。
func (r *Runner) RunSubset(ctx context.Context, targets []string) (*Report, error) {
	chosen := make(map[string]bool)
	for _, t := range targets {
		if _, ok := r.tasks[t]; !ok {
			return nil, &DependencyMissingError{Task: "(run targets)", Dependency: t}
		}
		var walk func(n string)
		walk = func(n string) {
			if chosen[n] {
				return
			}
			chosen[n] = true
			for _, d := range r.tasks[n].Deps {
				walk(d)
			}
		}
		walk(t)
	}
	names := make([]string, 0, len(chosen))
	for n := range chosen {
		names = append(names, n)
	}
	sort.Strings(names)
	return r.run(ctx, names)
}

func (r *Runner) run(ctx context.Context, chosen []string) (*Report, error) {
	selected := make(map[string]bool, len(chosen))
	for _, n := range chosen {
		selected[n] = true
	}

	// 为每个任务预分配结果槽；各 goroutine 只写自己名下的槽位，无需加锁。
	results := make(map[string]*Result, len(chosen))
	for _, n := range chosen {
		results[n] = &Result{Name: n}
	}

	sem := make(chan struct{}, r.opt.MaxConcurrency)
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	for _, level := range r.levels {
		var wg sync.WaitGroup
		for _, name := range level {
			if !selected[name] {
				continue
			}
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				r.executeOne(runCtx, sem, r.tasks[name], results)
			}(name)
		}
		wg.Wait()
	}

	return &Report{Results: results}, nil
}

// executeOne 计算单个任务的完整结论并写入其结果槽。
func (r *Runner) executeOne(
	runCtx context.Context,
	sem chan struct{},
	t Task,
	results map[string]*Result,
) {
	res := results[t.Name]

	// 1. 上游闸门：任一直接依赖未成功，本任务不执行。
	depKeys := make(map[string]string, len(t.Deps))
	for _, d := range t.Deps {
		dep := results[d]
		if !dep.Status.OK() {
			res.Status = StatusCanceled
			res.Err = &DependencyFailedError{
				Task:       t.Name,
				Dependency: d,
				Status:     dep.Status,
			}
			return
		}
		depKeys[d] = dep.Key
	}

	// 2. 结果标识：输入或任一依赖结果变化都会得到不同 Key。
	key := ComputeKey(t.Name, t.Input, depKeys)
	res.Key = key
	res.Deps = depKeys

	// 3. 缓存路径：命中必须校验条目自报标识，杜绝「输入变了却给旧结果」。
	if entry, found := r.store.Get(key); found {
		if entry.Key != key {
			res.Status = StatusFailed
			res.Err = &StaleCacheError{Task: t.Name, Key: key, StoredKey: entry.Key}
			return
		}
		res.Status = StatusCached
		res.Output = append([]byte(nil), entry.Output...)
		return
	}
	// 未命中：清理任何可能残留的同键记录（防御性）。
	r.store.Delete(key)

	// 4. 真实执行路径：先抢全局并发令牌，抢令牌期间同样响应取消/超时。
	select {
	case sem <- struct{}{}:
		defer func() { <-sem }()
	case <-runCtx.Done():
		res.Status, res.Err = classifyCtx(runCtx, runCtx, nil)
		return
	}

	taskCtx := runCtx
	cancelTask := func() {}
	if t.Timeout > 0 {
		taskCtx, cancelTask = context.WithTimeoutCause(runCtx, t.Timeout, ErrTimeout)
	}
	defer cancelTask()

	start := time.Now()
	output, err := t.Fn(taskCtx, t.Input, depOutputs(t.Deps, results))
	res.Dur = time.Since(start)

	status, cerr := classifyCtx(runCtx, taskCtx, err)
	res.Status = status
	res.Err = cerr
	if status == StatusSuccess {
		res.Output = output
		// 仅成功结果写入缓存：失败/超时/取消不污染后续运行。
		r.store.Put(key, CacheEntry{Key: key, Output: append([]byte(nil), output...)})
	}
}

// classifyCtx 统一判定终态：任务超时 > 运行取消/超时 > 函数普通失败 > 成功。
func classifyCtx(runCtx, taskCtx context.Context, fnErr error) (Status, error) {
	if context.Cause(taskCtx) == ErrTimeout {
		return StatusTimeout, ErrTimeout
	}
	switch runCtx.Err() {
	case context.Canceled:
		return StatusCanceled, ErrCanceled
	case context.DeadlineExceeded:
		return StatusTimeout, ErrTimeout
	}
	if fnErr != nil {
		return StatusFailed, fnErr
	}
	return StatusSuccess, nil
}

func depOutputs(deps []string, results map[string]*Result) map[string][]byte {
	out := make(map[string][]byte, len(deps))
	for _, d := range deps {
		out[d] = results[d].Output
	}
	return out
}
