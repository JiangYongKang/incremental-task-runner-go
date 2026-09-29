package runner_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	runner "github.com/highcumontoa/incremental-task-runner-go"
)

var errBoom = errors.New("boom")

// 部分失败：一个分支失败不能带偏无关分支；下游记为 upstream_failed 且原因可区分。
func TestPartialFailureIsolation(t *testing.T) {
	started := make(chan struct{}, 4)
	tasks := []*runner.Task{
		{ID: "good", Run: func(_ context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			started <- struct{}{}
			return runner.TaskOutput{Value: []byte("ok")}, nil
		}},
		{ID: "bad", Run: func(_ context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			started <- struct{}{}
			return runner.TaskOutput{}, errBoom
		}},
		{ID: "child", Deps: []runner.TaskID{"bad"}, Run: func(_ context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			t.Error("child must never run")
			return runner.TaskOutput{}, nil
		}},
	}
	r, err := runner.New(tasks, runner.WithConcurrency(4), runner.WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	rep, _ := r.Run(context.Background())
	if rep.Results["good"].Status != runner.StatusSucceeded {
		t.Fatalf("unrelated branch must succeed, got %s", rep.Results["good"].Status)
	}
	if rep.Results["bad"].Status != runner.StatusFailed || !errors.Is(rep.Results["bad"].Err, errBoom) {
		t.Fatalf("bad should be failed with errBoom, got %s err=%v", rep.Results["bad"].Status, rep.Results["bad"].Err)
	}
	if rep.Results["child"].Status != runner.StatusUpstreamFailed {
		t.Fatalf("child should be upstream_failed, got %s", rep.Results["child"].Status)
	}
	ue := rep.Results["child"].Err.(*runner.UpstreamFailureError)
	if ue.Upstream != "bad" || ue.UpstreamSt != runner.StatusFailed {
		t.Fatalf("upstream cause not distinguishable: %+v", ue)
	}
	t.Logf("判定依据: good=succeeded 不受影响; bad=failed(err=%v); child=upstream_failed(upstream=bad,status=failed)", errBoom)
}

// 取消：等待并发槽的任务应得到 canceled，而非 failed/timeout，且其执行体绝不运行。
func TestCancelWhileQueued(t *testing.T) {
	block := make(chan struct{})
	ids := []runner.TaskID{"b1", "b2", "b3"}
	entered := make(map[runner.TaskID]*int32, len(ids))
	tasks := make([]*runner.Task, 0, len(ids))
	for _, id := range ids {
		id := id
		n := new(int32)
		entered[id] = n
		tasks = append(tasks, &runner.Task{ID: id, Run: func(ctx context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			atomic.StoreInt32(n, 1)
			<-block
			if err := ctx.Err(); err != nil {
				return runner.TaskOutput{}, err
			}
			return runner.TaskOutput{Value: []byte(id)}, nil
		}})
	}
	r, err := runner.New(tasks, runner.WithConcurrency(2), runner.WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan *runner.Report)
	go func() { rep, _ := r.Run(ctx); done <- rep }()

	// 等待直到确定“两个任务占槽执行、一个任务排队”，避免对谁排队做假设。
	var queuedID runner.TaskID
	waitUntil(t, func() bool {
		snap := r.Snapshot()
		var running, pending []runner.TaskID
		for _, id := range ids {
			switch snap[id] {
			case runner.StatusRunning:
				running = append(running, id)
			case runner.StatusPending:
				pending = append(pending, id)
			}
		}
		if len(running) == 2 && len(pending) == 1 {
			queuedID = pending[0]
			return true
		}
		return false
	}, 2*time.Second)

	cancel()
	// 先确认排队任务在“无槽可用”的情况下被判定为 canceled，再释放槽位，
	// 避免槽位释放与取消同时就绪造成竞态。
	waitUntil(t, func() bool {
		return r.Snapshot()[queuedID] == runner.StatusCanceled
	}, 2*time.Second)
	close(block)
	rep := <-done

	if rep.Results[queuedID].Status != runner.StatusCanceled {
		t.Fatalf("queued task %s should be canceled, got %s", queuedID, rep.Results[queuedID].Status)
	}
	if atomic.LoadInt32(entered[queuedID]) != 0 {
		t.Fatalf("queued task %s must never execute", queuedID)
	}
	t.Logf("判定依据: 取消时排队中的 %s 状态=%s, 执行体未进入; 占槽任务为 b1/b2/b3 中的另两个", queuedID, rep.Results[queuedID].Status)
}

// 任务级超时：超时任务状态为 timeout，既不与 failed 也不与 canceled 混淆。
func TestTaskTimeout(t *testing.T) {
	tasks := []*runner.Task{
		{ID: "slow", Timeout: 30 * time.Millisecond, Run: func(ctx context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			select {
			case <-time.After(2 * time.Second):
				return runner.TaskOutput{Value: []byte("late")}, nil
			case <-ctx.Done():
				return runner.TaskOutput{}, ctx.Err()
			}
		}},
		{ID: "fast", Run: func(_ context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			return runner.TaskOutput{Value: []byte("fast")}, nil
		}},
	}
	r, err := runner.New(tasks, runner.WithConcurrency(4), runner.WithLogger(newTestLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	rep, _ := r.Run(context.Background())
	if rep.Results["slow"].Status != runner.StatusTimedOut {
		t.Fatalf("slow should time out, got %s", rep.Results["slow"].Status)
	}
	if rep.Results["fast"].Status != runner.StatusSucceeded {
		t.Fatalf("fast should succeed, got %s", rep.Results["fast"].Status)
	}
	t.Logf("判定依据: slow=%s (任务级 30ms 超时), fast=%s 不受影响", rep.Results["slow"].Status, rep.Results["fast"].Status)
}

// 父 context 超时与主动取消可区分（deadline -> timeout，cancel -> canceled）。
func TestParentDeadlineVsCancel(t *testing.T) {
	mk := func() []*runner.Task {
		return []*runner.Task{{ID: "h1", Run: func(ctx context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			<-ctx.Done()
			return runner.TaskOutput{}, ctx.Err()
		}}, {ID: "h2", Run: func(ctx context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			<-ctx.Done()
			return runner.TaskOutput{}, ctx.Err()
		}}}
	}
	r1, _ := runner.New(mk(), runner.WithConcurrency(1))
	ctx1, c1 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer c1()
	rep1, _ := r1.Run(ctx1)

	r2, _ := runner.New(mk(), runner.WithConcurrency(1))
	ctx2, c2 := context.WithCancel(context.Background())
	go func() { time.Sleep(30 * time.Millisecond); c2() }()
	rep2, _ := r2.Run(ctx2)

	if rep1.Results["h2"].Status != runner.StatusTimedOut {
		t.Fatalf("deadline exceeded should be timeout, got %s", rep1.Results["h2"].Status)
	}
	if rep2.Results["h2"].Status != runner.StatusCanceled {
		t.Fatalf("cancel should be canceled, got %s", rep2.Results["h2"].Status)
	}
	t.Logf("判定依据: 父超时 h2=%s; 父取消 h2=%s, 二者可区分", rep1.Results["h2"].Status, rep2.Results["h2"].Status)
}

// 失败/取消后重新执行：已成功且未失效的任务走缓存不重算，失败任务重新尝试。
func TestResumeAfterFailure(t *testing.T) {
	var aRuns, bRuns int32
	cache := runner.NewMemoryCache()
	flaky := int32(1) // 第一次失败
	mk := func() []*runner.Task {
		return []*runner.Task{
			{ID: "a", Input: []byte("in"), Run: func(_ context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
				atomic.AddInt32(&aRuns, 1)
				return runner.TaskOutput{Value: []byte("A")}, nil
			}},
			{ID: "b", Deps: []runner.TaskID{"a"}, Run: func(_ context.Context, in runner.TaskInput) (runner.TaskOutput, error) {
				n := atomic.AddInt32(&bRuns, 1)
				if n == 1 && flaky == 1 {
					return runner.TaskOutput{}, errBoom
				}
				return runner.TaskOutput{Value: []byte(string(in.Deps["a"]) + "B")}, nil
			}},
		}
	}
	r1, _ := runner.New(mk(), runner.WithCache(cache))
	rep1, _ := r1.Run(context.Background())
	if rep1.Results["a"].Status != runner.StatusSucceeded || rep1.Results["b"].Status != runner.StatusFailed {
		t.Fatalf("unexpected first run: a=%s b=%s", rep1.Results["a"].Status, rep1.Results["b"].Status)
	}
	r2, _ := runner.New(mk(), runner.WithCache(cache), runner.WithLogger(newTestLogger(t)))
	rep2, _ := r2.Run(context.Background())
	if atomic.LoadInt32(&aRuns) != 1 {
		t.Fatalf("succeeded a must be resumed from cache, runs=%d", aRuns)
	}
	if !rep2.Results["a"].Cached {
		t.Fatal("a should be served from cache on resume")
	}
	if rep2.Results["b"].Status != runner.StatusSucceeded || string(rep2.Results["b"].Value) != "AB" {
		t.Fatalf("b should succeed on retry, got %s value=%q", rep2.Results["b"].Status, rep2.Results["b"].Value)
	}
	t.Logf("判定依据: 重跑 a 命中缓存(执行次数=%d), b 重新执行并成功=%q", aRuns, rep2.Results["b"].Value)
}

// 运行中状态可被 Snapshot 观测到，证明 running 与其他状态区分。
func TestSnapshotObservesRunning(t *testing.T) {
	release := make(chan struct{})
	tasks := []*runner.Task{
		{ID: "running", Run: func(ctx context.Context, _ runner.TaskInput) (runner.TaskOutput, error) {
			select {
			case <-release:
				return runner.TaskOutput{Value: []byte("done")}, nil
			case <-ctx.Done():
				return runner.TaskOutput{}, ctx.Err()
			}
		}},
	}
	r, _ := runner.New(tasks, runner.WithConcurrency(1))
	go func() { r.Run(context.Background()) }()
	waitUntil(t, func() bool {
		s, ok := r.Snapshot()["running"]
		return ok && s == runner.StatusRunning
	}, 2*time.Second)
	close(release)
	t.Log("判定依据: Snapshot 在任务执行期间观测到 running 状态")
}
