package runner_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	runner "github.com/highcumontoa/incremental-task-runner-go"
)

// 跨 Run 复用同一缓存：已完成且未失效的任务不应被重算。
func TestCacheReuseAcrossRuns(t *testing.T) {
	var runs int32
	tasks := []*runner.Task{
		{ID: "a", Input: []byte("v1"), Run: func(_ context.Context, in runner.TaskInput) (runner.TaskOutput, error) {
			atomic.AddInt32(&runs, 1)
			return runner.TaskOutput{Value: []byte("A:" + string(in.Input))}, nil
		}},
	}
	cache := runner.NewMemoryCache()
	r, err := runner.New(tasks, runner.WithCache(cache), runner.WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	rep1, _ := r.Run(context.Background())
	if rep1.Results["a"].Status != runner.StatusSucceeded || rep1.Results["a"].Cached {
		t.Fatalf("first run should execute, got %+v", rep1.Results["a"])
	}
	rep2, _ := r.Run(context.Background())
	res := rep2.Results["a"]
	if !res.Cached {
		t.Fatal("second run should hit cache")
	}
	if atomic.LoadInt32(&runs) != 1 {
		t.Fatalf("task must not be recomputed, runs=%d", atomic.LoadInt32(&runs))
	}
	if string(res.Value) != "A:v1" {
		t.Fatalf("cache hit returned different value: %q", res.Value)
	}
	t.Logf("判定依据: task=a 第二次运行缓存命中, 执行次数=%d, 状态=succeeded, 值=%q", atomic.LoadInt32(&runs), res.Value)
}

// 输入变化：缓存必须失效并重新执行，新结果替换旧结果。
func TestCacheInvalidatedOnInputChange(t *testing.T) {
	var runs int32
	mk := func(input string) []*runner.Task {
		return []*runner.Task{{
			ID: "a", Input: []byte(input),
			Run: func(_ context.Context, in runner.TaskInput) (runner.TaskOutput, error) {
				atomic.AddInt32(&runs, 1)
				return runner.TaskOutput{Value: []byte("out:" + string(in.Input))}, nil
			},
		}}
	}
	cache := runner.NewMemoryCache()
	r1, _ := runner.New(mk("v1"), runner.WithCache(cache))
	if _, err := r1.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	r2, _ := runner.New(mk("v2"), runner.WithCache(cache), runner.WithLogger(newTestLogger(t)))
	rep, _ := r2.Run(context.Background())
	res := rep.Results["a"]
	if res.Cached {
		t.Fatal("changed input must invalidate cache")
	}
	if atomic.LoadInt32(&runs) != 2 {
		t.Fatalf("task should be rerun, runs=%d", atomic.LoadInt32(&runs))
	}
	if string(res.Value) != "out:v2" {
		t.Fatalf("stale result after input change: %q", res.Value)
	}
	t.Logf("判定依据: task=a 输入 v1->v2, 缓存失效重算, 新结果=%q, 执行次数=%d", res.Value, atomic.LoadInt32(&runs))
}

// 依赖结果变化：下游缓存必须失效。
func TestCacheInvalidatedOnDependencyChange(t *testing.T) {
	var aRuns, bRuns int32
	mk := func(aInput string) []*runner.Task {
		return []*runner.Task{
			{ID: "a", Input: []byte(aInput), Run: func(_ context.Context, in runner.TaskInput) (runner.TaskOutput, error) {
				atomic.AddInt32(&aRuns, 1)
				return runner.TaskOutput{Value: []byte("A=" + string(in.Input))}, nil
			}},
			{ID: "b", Deps: []runner.TaskID{"a"}, Run: func(_ context.Context, in runner.TaskInput) (runner.TaskOutput, error) {
				atomic.AddInt32(&bRuns, 1)
				return runner.TaskOutput{Value: []byte("B(" + string(in.Deps["a"]) + ")")}, nil
			}},
		}
	}
	cache := runner.NewMemoryCache()
	ra, _ := runner.New(mk("x"), runner.WithCache(cache))
	if _, err := ra.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	rb, _ := runner.New(mk("y"), runner.WithCache(cache), runner.WithLogger(newTestLogger(t)))
	rep, _ := rb.Run(context.Background())
	if atomic.LoadInt32(&aRuns) != 2 {
		t.Fatalf("a should rerun, runs=%d", atomic.LoadInt32(&aRuns))
	}
	if atomic.LoadInt32(&bRuns) != 2 {
		t.Fatalf("b must rerun because dependency result changed, runs=%d", atomic.LoadInt32(&bRuns))
	}
	if string(rep.Results["b"].Value) != "B(A=y)" {
		t.Fatalf("unexpected b value: %q", rep.Results["b"].Value)
	}
	t.Logf("判定依据: a 结果变化 -> b 的依赖指纹变化 -> b 缓存失效重算, b=%q", rep.Results["b"].Value)
}

// 命中缓存与实际执行两条路径对外结论一致（状态与值）。
func TestCacheHitEquivalentToExecution(t *testing.T) {
	tasks := func() []*runner.Task {
		return []*runner.Task{{ID: "a", Input: []byte("same"), Run: okFunc}}
	}
	ra, _ := runner.New(tasks())
	repA, _ := ra.Run(context.Background())
	cache := runner.NewMemoryCache()
	rb, _ := runner.New(tasks(), runner.WithCache(cache))
	rb.Run(context.Background())
	rc, _ := runner.New(tasks(), runner.WithCache(cache))
	repC, _ := rc.Run(context.Background())
	got, want := repC.Results["a"], repA.Results["a"]
	if got.Status != want.Status || string(got.Value) != string(want.Value) {
		t.Fatalf("cache path result differs: got=%+v want=%+v", got, want)
	}
	t.Logf("判定依据: 缓存路径与执行路径结论一致 status=%s value=%q", got.Status, got.Value)
}

// tamperCache 在首次写入后篡改 Value，模拟“旧结果/损坏条目被当作命中”。
type tamperCache struct {
	inner runner.Cache
}

func (c *tamperCache) Get(id runner.TaskID) (runner.CacheEntry, bool) {
	return c.inner.Get(id)
}
func (c *tamperCache) Put(id runner.TaskID, e runner.CacheEntry) {
	if id == "a" {
		e.Value = []byte("TAMPERED-VALUE") // 与 ResultFP 不再匹配
	}
	c.inner.Put(id, e)
}

// 改了输入却返回旧结果 / 缓存条目损坏：必须以 CacheConsistencyError 失败，不得静默成功。
func TestCacheConsistencyViolationFails(t *testing.T) {
	cache := &tamperCache{inner: runner.NewMemoryCache()}
	var runs int32
	tasks := []*runner.Task{{ID: "a", Input: []byte("v1"), Run: func(_ context.Context, in runner.TaskInput) (runner.TaskOutput, error) {
		atomic.AddInt32(&runs, 1)
		return runner.TaskOutput{Value: []byte("real")}, nil
	}}}
	r, _ := runner.New(tasks, runner.WithCache(cache), runner.WithLogger(newTestLogger(t)))
	r.Run(context.Background())
	rep, _ := r.Run(context.Background())
	res := rep.Results["a"]
	if res.Status != runner.StatusFailed {
		t.Fatalf("expected failed, got %s", res.Status)
	}
	var ce *runner.CacheConsistencyError
	if !errors.As(res.Err, &ce) || ce.ID != "a" {
		t.Fatalf("expected *CacheConsistencyError for a, got %v", res.Err)
	}
	t.Logf("判定依据: task=a 缓存条目的值与结果指纹不一致, 拒绝静默命中, err=%v", res.Err)
}
