package runner

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// TaskResult 是单个任务在一次 Run 中的最终结论。
type TaskResult struct {
	ID     TaskID
	Status Status
	Value  []byte
	Err    error
	// Cached 为 true 表示结论来自缓存命中，未实际执行任务函数；
	// 但对外的 Status/Value 与实际执行路径完全一致。
	Cached bool
}

// Report 是一次 Run 的完整结论，每个任务都有独立结论，互不带偏。
type Report struct {
	Results map[TaskID]*TaskResult
	// Order 为确定性的任务顺序（拓扑序），便于可复现地遍历结论。
	Order []TaskID
}

// Runner 是增量任务执行器，缓存可在多次 Run 之间复用，
// 因此重新执行时已完成且未失效的任务直接从缓存恢复，不会重算。
type Runner struct {
	g              *graph
	cache          Cache
	logger         Logger
	concurrency    int
	defaultTimeout time.Duration

	// runMu 防止两次 Run 并发执行造成状态互相污染。
	runMu  sync.Mutex
	mu     sync.Mutex
	states map[TaskID]Status
}

// Option 配置 Runner。
type Option func(*Runner)

// WithConcurrency 配置整体并发上限（同一时刻实际执行的任务数）。
func WithConcurrency(n int) Option {
	return func(r *Runner) { r.concurrency = n }
}

// WithCache 注入缓存存储；多次 Run 复用同一 Cache 即可实现增量续跑。
func WithCache(c Cache) Option {
	return func(r *Runner) { r.cache = c }
}

// WithLogger 注入日志器。
func WithLogger(l Logger) Option {
	return func(r *Runner) { r.logger = l }
}

// WithDefaultTimeout 配置所有任务的默认超时；任务自身的非零 Timeout 优先。
func WithDefaultTimeout(d time.Duration) Option {
	return func(r *Runner) { r.defaultTimeout = d }
}

// New 校验任务声明（重复/缺失/成环/非法）并构造 Runner。
// 任何声明问题都以可区分的错误类型返回，不会被静默接受。
func New(tasks []*Task, opts ...Option) (*Runner, error) {
	g, err := buildGraph(tasks)
	if err != nil {
		return nil, err
	}
	r := &Runner{
		g:              g,
		cache:          NewMemoryCache(),
		logger:         discardLogger{},
		concurrency:    runtime.NumCPU(),
		defaultTimeout: 0,
		states:         make(map[TaskID]Status),
	}
	for _, opt := range opts {
		opt(r)
	}
	if r.concurrency <= 0 {
		return nil, &InvalidTaskError{Reason: fmt.Sprintf("concurrency must be positive, got %d", r.concurrency)}
	}
	if r.defaultTimeout < 0 {
		return nil, &InvalidTaskError{Reason: "default timeout must be >= 0"}
	}
	for _, t := range g.tasks {
		if t.Timeout < 0 {
			return nil, &InvalidTaskError{ID: t.ID, Reason: "task timeout must be >= 0"}
		}
	}
	for _, id := range g.order {
		r.states[id] = StatusPending
	}
	return r, nil
}

// Run 执行（或从缓存恢复）所有任务。
// 声明期错误在 New 阶段返回；运行期每个任务的结论都体现在 Report 中，
// 父 context 的取消/超时同样体现为相关任务的状态，而不是返回 error。
func (r *Runner) Run(ctx context.Context) (*Report, error) {
	r.runMu.Lock()
	defer r.runMu.Unlock()

	r.mu.Lock()
	r.states = make(map[TaskID]Status, len(r.g.order))
	for _, id := range r.g.order {
		r.states[id] = StatusPending
	}
	r.mu.Unlock()

	sem := make(chan struct{}, r.concurrency)
	done := make(map[TaskID]chan struct{}, len(r.g.order))
	results := make(map[TaskID]*TaskResult, len(r.g.order))
	var resMu sync.Mutex
	for _, id := range r.g.order {
		done[id] = make(chan struct{})
	}

	setState := func(id TaskID, s Status) {
		r.mu.Lock()
		r.states[id] = s
		r.mu.Unlock()
	}

	var wg sync.WaitGroup
	for _, id := range r.g.order {
		wg.Add(1)
		go func(id TaskID) {
			defer wg.Done()
			defer close(done[id])

			// 1. 等待全部直接依赖结束（无论成功与否，channel 都会关闭）。
			for _, d := range r.g.deps[id] {
				<-done[d]
			}

			// 2. 依赖若有非成功结论，本任务不执行，记为上游失败，
			//    并携带具体是哪个上游、什么状态，原因可区分。
			resMu.Lock()
			var badDep TaskID
			var badSt Status
			for _, d := range r.g.deps[id] {
				if dr := results[d]; dr != nil && dr.Status != StatusSucceeded {
					badDep, badSt = d, dr.Status
					break
				}
			}
			resMu.Unlock()
			if badDep != "" {
				res := &TaskResult{
					ID:     id,
					Status: StatusUpstreamFailed,
					Err:    &UpstreamFailureError{TaskID: id, Upstream: badDep, UpstreamSt: badSt},
				}
				resMu.Lock()
				results[id] = res
				resMu.Unlock()
				setState(id, StatusUpstreamFailed)
				r.logger.Logf("task %s: skipped as upstream_failed (upstream=%s upstream_status=%s)", id, badDep, badSt)
				return
			}

			// 3. 汇总依赖结果（仅成功依赖会到达这里）。所有 results 读写均在 resMu 下。
			depValues := make(map[TaskID][]byte, len(r.g.deps[id]))
			depResultFP := make(map[TaskID]fingerprint, len(r.g.deps[id]))
			resMu.Lock()
			for _, d := range r.g.deps[id] {
				dr := results[d]
				depValues[d] = append([]byte(nil), dr.Value...)
				depResultFP[d] = fingerprint(hashBytes(dr.Value))
			}
			resMu.Unlock()
			inputFP := hashBytes(r.g.tasks[id].Input)
			depsFP := hashDepResults(depResultFP)

			// 4. 缓存判定：输入或依赖结果指纹变化即失效，必须重算。
			if entry, ok := r.cache.Get(id); ok {
				switch {
				case entry.InputFP == inputFP && entry.DepsFP == depsFP:
					if fp := fingerprint(hashBytes(entry.Value)); fp != entry.ResultFP {
						// 声称命中，但条目内容与其结果指纹不一致：旧结果/损坏条目，
						// 绝不能当作成功结论静默返回。
						fail := &TaskResult{ID: id, Status: StatusFailed, Err: &CacheConsistencyError{ID: id}}
						resMu.Lock()
						results[id] = fail
						resMu.Unlock()
						setState(id, StatusFailed)
						r.logger.Logf("task %s: cache consistency violation (entry result fingerprint mismatch)", id)
						return
					}
					res := &TaskResult{
						ID:     id,
						Status: StatusSucceeded,
						Value:  append([]byte(nil), entry.Value...),
						Cached: true,
					}
					resMu.Lock()
					results[id] = res
					resMu.Unlock()
					setState(id, StatusSucceeded)
					r.logger.Logf("task %s: cache hit (inputFP=%s depsFP=%s status=succeeded)", id, inputFP, depsFP)
					return
				default:
					r.logger.Logf("task %s: cache invalidated (reason=input or dependency result changed; inputFP=%s depsFP=%s)", id, inputFP, depsFP)
					// 旧条目作废，落到实际执行路径，成功后覆盖。
				}
			}

			// 5. 获取并发槽位；排队期间父 context 结束也要给出明确状态。
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				st := classifyContext(ctx)
				res := &TaskResult{ID: id, Status: st, Err: ctx.Err()}
				resMu.Lock()
				results[id] = res
				resMu.Unlock()
				setState(id, st)
				r.logger.Logf("task %s: %s while waiting for concurrency slot", id, st)
				return
			}
			semaphoreHeld := true
			release := func() {
				if semaphoreHeld {
					<-sem
					semaphoreHeld = false
				}
			}
			defer release()

			// 获取槽位后父 context 可能已经结束。
			if err := ctx.Err(); err != nil {
				st := classifyContext(ctx)
				res := &TaskResult{ID: id, Status: st, Err: err}
				resMu.Lock()
				results[id] = res
				resMu.Unlock()
				setState(id, st)
				r.logger.Logf("task %s: %s before start", id, st)
				return
			}

			// 6. 建立任务超时 context（任务级 Timeout 优先于默认值，0 表示不限）。
			taskCtx := ctx
			var cancel context.CancelFunc
			timeout := r.g.tasks[id].Timeout
			if timeout == 0 {
				timeout = r.defaultTimeout
			}
			if timeout > 0 {
				taskCtx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			setState(id, StatusRunning)
			r.logger.Logf("task %s: running (concurrency slot acquired, timeout=%s)", id, timeoutOrNone(timeout))
			out, runErr := r.g.tasks[id].Run(taskCtx, TaskInput{
				ID:    id,
				Input: append([]byte(nil), r.g.tasks[id].Input...),
				Deps:  depValues,
			})

			// 7. 结论分类：成功 / 失败 / 超时 / 取消，四类严格区分。
			if runErr == nil {
				value := append([]byte(nil), out.Value...)
				entry := CacheEntry{
					InputFP:  inputFP,
					DepsFP:   depsFP,
					ResultFP: fingerprint(hashBytes(value)),
					Value:    value,
				}
				r.cache.Put(id, entry)
				res := &TaskResult{ID: id, Status: StatusSucceeded, Value: value}
				resMu.Lock()
				results[id] = res
				resMu.Unlock()
				setState(id, StatusSucceeded)
				r.logger.Logf("task %s: succeeded after execution (resultFP=%s, cached_entry_stored)", id, entry.ResultFP)
				return
			}

			st := StatusFailed
			switch {
			case taskCtx.Err() == context.DeadlineExceeded && ctx.Err() != context.DeadlineExceeded:
				// 任务自身超时，父 context 未整体超时。
				st = StatusTimedOut
			case ctx.Err() != nil:
				st = classifyContext(ctx)
			case taskCtx.Err() == context.DeadlineExceeded:
				st = StatusTimedOut
			}
			res := &TaskResult{ID: id, Status: st, Err: runErr}
			resMu.Lock()
			results[id] = res
			resMu.Unlock()
			setState(id, st)
			r.logger.Logf("task %s: %s (err=%q)", id, st, runErr.Error())
		}(id)
	}
	wg.Wait()

	report := &Report{Results: results, Order: append([]TaskID(nil), r.g.order...)}
	r.logger.Logf("run finished: %d tasks concluded", len(r.g.order))
	return report, nil
}

// Snapshot 返回当前各任务状态的副本，可用于观测 running 等中间状态。
func (r *Runner) Snapshot() map[TaskID]Status {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[TaskID]Status, len(r.states))
	for id, s := range r.states {
		out[id] = s
	}
	return out
}

// classifyContext 区分父 context 是被取消还是整体超时。
func classifyContext(ctx context.Context) Status {
	if ctx.Err() == context.DeadlineExceeded {
		return StatusTimedOut
	}
	return StatusCanceled
}

func timeoutOrNone(d time.Duration) string {
	if d <= 0 {
		return "none"
	}
	return d.String()
}
