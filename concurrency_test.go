package runner_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	runner "github.com/highcumontoa/incremental-task-runner-go"
)

// 并发上限：同一时刻正在执行的任务数绝不能超过配置值；同层任务确实并发。
func TestConcurrencyLimit(t *testing.T) {
	const limit = 2
	var inFlight, maxInFlight int32
	released := make(chan struct{})
	tasks := make([]*runner.Task, 0, 6)
	for i := 0; i < 6; i++ {
		id := runner.TaskID("t" + string(rune('0'+i)))
		tasks = append(tasks, &runner.Task{
			ID: id,
			Run: func(_ context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
				cur := atomic.AddInt32(&inFlight, 1)
				for {
					old := atomic.LoadInt32(&maxInFlight)
					if cur <= old || atomic.CompareAndSwapInt32(&maxInFlight, old, cur) {
						break
					}
				}
				<-released
				atomic.AddInt32(&inFlight, -1)
				return runner.TaskOutput{Value: []byte(id)}, nil
			},
		})
	}
	r, err := runner.New(tasks, runner.WithConcurrency(limit), runner.WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		r.Run(context.Background())
		close(done)
	}()
	// 等待任务把并发槽占满。
	waitUntil(t, func() bool { return atomic.LoadInt32(&maxInFlight) >= limit }, 2*time.Second)
	time.Sleep(80 * time.Millisecond)
	if got := atomic.LoadInt32(&inFlight); got != limit {
		t.Fatalf("expected %d in flight, got %d", limit, got)
	}
	if got := atomic.LoadInt32(&maxInFlight); got > limit {
		t.Fatalf("concurrency limit exceeded: max=%d limit=%d", got, limit)
	}
	close(released)
	<-done
	if max := atomic.LoadInt32(&maxInFlight); max != limit {
		t.Fatalf("expected tasks to run concurrently up to %d, observed max=%d", limit, max)
	}
	t.Logf("判定依据: 6 个同层任务, 上限=%d, 观测最大并发=%d, 未超限", limit, maxInFlight)
}

// 层间调度：同层并发、不同层按依赖顺序执行，依赖结果正确传递。
func TestLayeredSchedulingAndDependencyValues(t *testing.T) {
	var ranA, ranB, ranC int32
	tasks := []*runner.Task{
		{ID: "c", Deps: []runner.TaskID{"a", "b"}, Run: func(_ context.Context, in runner.TaskInput) (runner.TaskOutput, error) {
			atomic.StoreInt32(&ranC, 1)
			if atomic.LoadInt32(&ranA) != 1 || atomic.LoadInt32(&ranB) != 1 {
				t.Errorf("c ran before its dependencies")
			}
			return runner.TaskOutput{Value: append(append([]byte(nil), in.Deps["a"]...), in.Deps["b"]...)}, nil
		}},
		{ID: "a", Run: func(_ context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			atomic.StoreInt32(&ranA, 1)
			return runner.TaskOutput{Value: []byte("A")}, nil
		}},
		{ID: "b", Run: func(_ context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			atomic.StoreInt32(&ranB, 1)
			return runner.TaskOutput{Value: []byte("B")}, nil
		}},
	}
	r, err := runner.New(tasks, runner.WithConcurrency(4), runner.WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	rep, _ := r.Run(context.Background())
	if string(rep.Results["c"].Value) != "AB" {
		t.Fatalf("expected c=AB, got %q", rep.Results["c"].Value)
	}
	t.Logf("判定依据: a,b 先于 c 执行且结果作为依赖传入, c=%q", rep.Results["c"].Value)
}

// 可复现：重复运行，报告顺序与各任务结论一致。
func TestDeterministicReport(t *testing.T) {
	mk := func() []*runner.Task {
		return []*runner.Task{
			{ID: "z", Deps: []runner.TaskID{"x"}, Run: okFunc},
			{ID: "x", Run: okFunc},
			{ID: "y", Deps: []runner.TaskID{"x"}, Run: okFunc},
		}
	}
	r1, _ := runner.New(mk())
	r2, _ := runner.New(mk())
	rep1, _ := r1.Run(context.Background())
	rep2, _ := r2.Run(context.Background())
	if len(rep1.Order) != len(rep2.Order) {
		t.Fatal("report order length differs")
	}
	for i := range rep1.Order {
		if rep1.Order[i] != rep2.Order[i] {
			t.Fatalf("nondeterministic order: %v vs %v", rep1.Order, rep2.Order)
		}
	}
	for id, res := range rep1.Results {
		if res.Status != rep2.Results[id].Status || string(res.Value) != string(rep2.Results[id].Value) {
			t.Fatalf("nondeterministic result for %s", id)
		}
	}
	t.Logf("判定依据: 两次运行报告顺序一致 order=%v", rep1.Order)
}

func waitUntil(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}
