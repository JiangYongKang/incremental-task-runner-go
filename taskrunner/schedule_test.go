package taskrunner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// 同层任务并发执行，但真实执行的在跑数不得超过 MaxConcurrency。
func TestSchedule_ConcurrencyCap(t *testing.T) {
	const n = 8
	const capN = 3
	var inFlight, maxObserved int32
	tasks := make([]Task, 0, n)
	release := make(chan struct{})
	entered := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		name := string(rune('a' + i))
		fn := func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
			cur := atomic.AddInt32(&inFlight, 1)
			for {
				old := atomic.LoadInt32(&maxObserved)
				if cur <= old || atomic.CompareAndSwapInt32(&maxObserved, old, cur) {
					break
				}
			}
			entered <- struct{}{}
			<-release
			atomic.AddInt32(&inFlight, -1)
			return in, nil
		}
		tasks = append(tasks, Task{Name: name, Fn: fn})
	}

	r, _ := New(tasks, NewMemoryStore(), Options{MaxConcurrency: capN})
	done := make(chan struct{})
	go func() {
		r.Run(context.Background())
		close(done)
	}()

	// 恰好 capN 个任务先进入执行，第 capN+1 个必须等令牌。
	for i := 0; i < capN; i++ {
		<-entered
	}
	select {
	case <-entered:
		t.Fatalf("判定依据=并发上限 %d 被突破：出现第 %d 个并发执行", capN, capN+1)
	case <-time.After(50 * time.Millisecond):
	}
	if got := atomic.LoadInt32(&maxObserved); got != capN {
		t.Fatalf("判定依据=峰值并发应=%d, 实际=%d", capN, got)
	}
	close(release)
	<-done
	t.Logf("判定依据=峰值并发=%d，未超过上限 MaxConcurrency=%d", maxObserved, capN)
}

// 运行级取消：运行中的任务得 canceled；未轮到/被阻塞的下游同样 canceled，
// 已在更早层完成的任务结论不受影响。
func TestSchedule_Cancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered1 := make(chan struct{}, 1)
	entered2 := make(chan struct{}, 1)
	release := make(chan struct{})

	tasks := []Task{
		// 第 0 层独占：保证在取消前完成并保留成功结论。
		{Name: "done-early", Input: []byte("e"), Fn: okFn},
		// 第 1 层两个阻塞任务 + 并发上限 1：恰好一个持令牌运行，另一个等令牌。
		{Name: "running", Deps: []string{"done-early"}, Fn: blockingFn(entered1, release)},
		{Name: "lonely", Deps: []string{"done-early"}, Fn: blockingFn(entered2, release)},
		// 第 2 层：被上游取消带累的下游。
		{Name: "waiting", Deps: []string{"running"}, Fn: okFn},
	}
	// MaxConcurrency=1：lonely/running 中必有一个被挡在令牌之后。
	r, _ := New(tasks, NewMemoryStore(), Options{MaxConcurrency: 1})

	resCh := make(chan *Report, 1)
	go func() {
		rep, _ := r.Run(ctx)
		resCh <- rep
	}()
	// 等待恰好一个任务进入执行（持令牌者），另一个确定在等令牌。
	select {
	case <-entered1:
	case <-entered2:
	case <-time.After(2 * time.Second):
		t.Fatalf("判定依据=应有任务进入执行")
	}
	cancel()
	rep := <-resCh

	if rep.Get("done-early").Status != StatusSuccess {
		t.Fatalf("task=<done-early> 判定依据=取消前完成的任务应保持成功, 实际=%s",
			rep.Get("done-early").Status)
	}
	running := rep.Get("running")
	if running.Status != StatusCanceled || !errors.Is(running.Err, ErrCanceled) {
		t.Fatalf("task=<running> 判定依据=应为 canceled/ErrCanceled, 实际=%s err=%v",
			running.Status, running.Err)
	}
	waiting := rep.Get("waiting")
	if waiting.Status != StatusCanceled {
		t.Fatalf("task=<waiting> 判定依据=被上游取消带累的下游应为 canceled, 实际=%s err=%v",
			waiting.Status, waiting.Err)
	}
	lonely := rep.Get("lonely")
	if lonely.Status != StatusCanceled {
		t.Fatalf("task=<lonely> 判定依据=未抢到令牌的无关任务应记 canceled, 实际=%s err=%v",
			lonely.Status, lonely.Err)
	}
	t.Logf("判定依据=取消生效: done-early=%s running=%s waiting=%s lonely=%s",
		rep.Get("done-early").Status, running.Status, waiting.Status, lonely.Status)
}

// 任务级超时与运行级超时都必须得到 StatusTimeout，与 canceled/failed 区分。
func TestSchedule_Timeout(t *testing.T) {
	t.Run("task-timeout", func(t *testing.T) {
		tasks := []Task{
			{Name: "slow", Timeout: 20 * time.Millisecond, Fn: slowFn(2 * time.Second)},
			{Name: "after", Deps: []string{"slow"}, Fn: okFn},
		}
		r, _ := New(tasks, NewMemoryStore(), Options{MaxConcurrency: 2})
		rep, _ := r.Run(context.Background())
		slow := rep.Get("slow")
		if slow.Status != StatusTimeout || !errors.Is(slow.Err, ErrTimeout) {
			t.Fatalf("task=<slow> 判定依据=应为 timeout/ErrTimeout, 实际=%s err=%v",
				slow.Status, slow.Err)
		}
		if rep.Get("after").Status != StatusCanceled {
			t.Fatalf("task=<after> 判定依据=超时任务的下游应被阻塞取消, 实际=%s",
				rep.Get("after").Status)
		}
		t.Logf("task=<slow> 判定依据=任务级超时 status=%s dur=%s, 下游 after=%s",
			slow.Status, slow.Dur, rep.Get("after").Status)
	})

	t.Run("run-context-timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		tasks := []Task{{Name: "slow", Fn: slowFn(2 * time.Second)}}
		r, _ := New(tasks, NewMemoryStore(), Options{MaxConcurrency: 1})
		rep, _ := r.Run(ctx)
		slow := rep.Get("slow")
		if slow.Status != StatusTimeout {
			t.Fatalf("task=<slow> 判定依据=运行级超时应记 timeout, 实际=%s err=%v",
				slow.Status, slow.Err)
		}
		t.Logf("task=<slow> 判定依据=运行级超时 status=%s", slow.Status)
	})
}

// 普通失败不能被误判为超时或取消。
func TestSchedule_FailureIsDistinct(t *testing.T) {
	tasks := []Task{{Name: "boom", Fn: func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
		return nil, errSentinelKaboom
	}}}
	r, _ := New(tasks, NewMemoryStore(), Options{})
	rep, _ := r.Run(context.Background())
	res := rep.Get("boom")
	if res.Status != StatusFailed || !errors.Is(res.Err, errSentinelKaboom) {
		t.Fatalf("task=<boom> 判定依据=应为 failed 且保留原错, 实际=%s err=%v", res.Status, res.Err)
	}
	if errors.Is(res.Err, ErrTimeout) || errors.Is(res.Err, ErrCanceled) {
		t.Fatalf("task=<boom> 判定依据=普通失败不得与超时/取消混淆")
	}
	t.Logf("task=<boom> 判定依据=普通失败 status=%s err=%v", res.Status, res.Err)
}

// RunSubset 只跑目标及其反向依赖闭包。
func TestSchedule_RunSubset(t *testing.T) {
	store := NewMemoryStore()
	var calls int32
	fn := countingFn(&calls, nil)
	tasks := []Task{
		{Name: "a", Fn: fn},
		{Name: "b", Deps: []string{"a"}, Fn: fn},
		{Name: "c", Deps: []string{"a"}, Fn: fn},
		{Name: "d", Deps: []string{"b", "c"}, Fn: fn},
	}
	r, _ := New(tasks, store, Options{MaxConcurrency: 4})

	rep, err := r.RunSubset(context.Background(), []string{"c"})
	if err != nil {
		t.Fatalf("判定依据=RunSubset 构造失败: %v", err)
	}
	if rep.Get("a").Status != StatusSuccess || rep.Get("c").Status != StatusSuccess {
		t.Fatalf("判定依据=闭包 {a,c} 应成功, a=%s c=%s", rep.Get("a").Status, rep.Get("c").Status)
	}
	if rep.Get("b") != nil || rep.Get("d") != nil {
		t.Fatalf("判定依据=闭包外任务不应出现在 Report 中, b=%v d=%v", rep.Get("b"), rep.Get("d"))
	}
	if calls != 2 {
		t.Fatalf("判定依据=仅应真实执行 a,c, 实际=%d", calls)
	}

	// 接着全量跑：a,c 命中缓存，b,d 首跑，实现从上次状态继续。
	rep2, _ := r.Run(context.Background())
	if rep2.Get("a").Status != StatusCached || rep2.Get("c").Status != StatusCached {
		t.Fatalf("判定依据=a,c 应命中缓存, a=%s c=%s", rep2.Get("a").Status, rep2.Get("c").Status)
	}
	if rep2.Get("b").Status != StatusSuccess || rep2.Get("d").Status != StatusSuccess {
		t.Fatalf("判定依据=b,d 应首次成功, b=%s d=%s", rep2.Get("b").Status, rep2.Get("d").Status)
	}
	t.Logf("判定依据=子集闭包 {a,c} 执行后全量继续: a,c=%s b,d=%s",
		rep2.Get("a").Status, rep2.Get("b").Status)

	if _, err := r.RunSubset(context.Background(), []string{"nope"}); !errors.Is(err, ErrDependencyMissing) {
		t.Fatalf("判定依据=未知目标应返回 ErrDependencyMissing, 实际=%v", err)
	}
}
