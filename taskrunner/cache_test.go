package taskrunner

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

// 命中缓存与真实执行两条路径对外结论一致：同状态族、同输出、同 Key。
func TestCache_HitPathMatchesRunPath(t *testing.T) {
	store := NewMemoryStore()
	tasks := []Task{
		{Name: "a", Input: []byte("v1"), Fn: okFn},
	}

	r1, err := New(tasks, store, Options{MaxConcurrency: 4})
	if err != nil {
		t.Fatalf("判定依据=构造失败: %v", err)
	}
	rep1, _ := r1.Run(context.Background())
	first := rep1.Get("a")
	if first.Status != StatusSuccess {
		t.Fatalf("task=<a> 判定依据=首次应真实成功, 实际=%s err=%v", first.Status, first.Err)
	}

	// 同一 Runner 再跑：应命中缓存。
	rep2, _ := r1.Run(context.Background())
	cached := rep2.Get("a")
	if cached.Status != StatusCached {
		t.Fatalf("task=<a> 判定依据=第二次应命中缓存, 实际=%s", cached.Status)
	}
	if cached.Key != first.Key {
		t.Fatalf("task=<a> 判定依据=缓存命中 Key 应一致 %s != %s", cached.Key, first.Key)
	}
	if !bytes.Equal(cached.Output, first.Output) {
		t.Fatalf("task=<a> 判定依据=缓存输出应与真实执行一致 %q != %q", cached.Output, first.Output)
	}
	if cached.Status.OK() != first.Status.OK() {
		t.Fatalf("task=<a> 判定依据=两条路径成功结论必须一致")
	}
	t.Logf("task=<a> 判定依据=缓存命中 status=%s key=%s output=%q", cached.Name, cached.Key, cached.Output)
}

// 输入变化 => Key 变化 => 旧缓存失效，必须真实重算，绝不能给旧结果。
func TestCache_InvalidatesOnInputChange(t *testing.T) {
	store := NewMemoryStore()
	var calls int32
	fn := countingFn(&calls, nil)
	tasks := []Task{{Name: "a", Input: []byte("input-1"), Fn: fn}}

	r, _ := New(tasks, store, Options{MaxConcurrency: 2})
	rep1, _ := r.Run(context.Background())
	key1 := rep1.Get("a").Key

	// 改输入后重建 Runner（同一缓存存储），模拟「代码没变、输入变了」。
	tasks[0].Input = []byte("input-2")
	r2, _ := New(tasks, store, Options{MaxConcurrency: 2})
	rep2, _ := r2.Run(context.Background())
	res2 := rep2.Get("a")

	if res2.Status != StatusSuccess {
		t.Fatalf("task=<a> 判定依据=输入变更后应真实重算成功, 实际=%s err=%v", res2.Status, res2.Err)
	}
	if res2.Key == key1 {
		t.Fatalf("task=<a> 判定依据=输入变更后 Key 必须变化, 旧 Key=%s", key1)
	}
	if string(res2.Output) != "input-2" {
		t.Fatalf("task=<a> 判定依据=改了输入还命中旧结果, output=%q", res2.Output)
	}
	if calls != 2 {
		t.Fatalf("task=<a> 判定依据=输入变更应触发一次真实执行, 实际调用=%d", calls)
	}
	t.Logf("task=<a> 判定依据=输入变化旧 Key=%s 新 Key=%s, 真实调用=%d", key1, res2.Key, calls)
}

// 依赖结果变化（这里由依赖输入变化引起）=> 下游 Key 必须变化并重算。
func TestCache_InvalidatesOnDependencyResultChange(t *testing.T) {
	store := NewMemoryStore()
	var downCalls int32
	mkTasks := func(depInput string) []Task {
		return []Task{
			{Name: "base", Input: []byte(depInput), Fn: okFn},
			{Name: "down", Deps: []string{"base"}, Fn: countingFn(&downCalls, nil)},
		}
	}

	r1, _ := New(mkTasks("dep-v1"), store, Options{MaxConcurrency: 2})
	rep1, _ := r1.Run(context.Background())
	keyDown1 := rep1.Get("down").Key

	r2, _ := New(mkTasks("dep-v2"), store, Options{MaxConcurrency: 2})
	rep2, _ := r2.Run(context.Background())
	res := rep2.Get("down")
	if res.Key == keyDown1 {
		t.Fatalf("task=<down> 判定依据=依赖结果变化后下游 Key 必须变化, 旧 Key=%s", keyDown1)
	}
	if downCalls != 2 {
		t.Fatalf("task=<down> 判定依据=依赖结果变化应触发下游重算, 调用=%d", downCalls)
	}
	t.Logf("task=<down> 判定依据=依赖 base 结果变化, 旧 Key=%s 新 Key=%s", keyDown1, res.Key)
}

// 防御性场景：缓存存储若按新 Key 返回了携带旧标识的条目，必须以失败论处。
func TestCache_StaleEntryIsFailure(t *testing.T) {
	store := NewMemoryStore()
	r, _ := New([]Task{{Name: "a", Input: []byte("in"), Fn: okFn}}, store, Options{})
	realKey := ComputeKey("a", []byte("in"), nil)
	store.Put(realKey, CacheEntry{Key: "old-key", Output: []byte("stale-output")})

	rep, _ := r.Run(context.Background())
	res := rep.Get("a")
	if !errors.Is(res.Err, ErrStaleCache) {
		t.Fatalf("task=<a> 判定依据=期望 ErrStaleCache, 实际 status=%s err=%v", res.Status, res.Err)
	}
	if res.Status == StatusCached || res.Status.OK() {
		t.Fatalf("task=<a> 判定依据=旧缓存命中绝不能算成功, status=%s", res.Status)
	}
	t.Logf("task=<a> 判定依据=篡改缓存条目被拒绝: %v", res.Err)
}

// 重新执行时干净继续：成功任务不重算；失败任务重跑；失败转成功后下游恢复。
func TestCache_ResumeAfterFailure(t *testing.T) {
	store := NewMemoryStore()
	var baseCalls, failCalls, downCalls int32
	var shouldFail atomic.Bool
	shouldFail.Store(true)

	flaky := func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
		atomic.AddInt32(&failCalls, 1)
		if shouldFail.Load() {
			return nil, errors.New("boom")
		}
		return in, nil
	}
	mkTasks := func() []Task {
		return []Task{
			{Name: "base", Input: []byte("b"), Fn: countingFn(&baseCalls, nil)},
			{Name: "flaky", Deps: []string{"base"}, Input: []byte("f"), Fn: flaky},
			{Name: "down", Deps: []string{"flaky"}, Input: []byte("d"), Fn: countingFn(&downCalls, nil)},
		}
	}

	r, _ := New(mkTasks(), store, Options{MaxConcurrency: 2})
	rep1, _ := r.Run(context.Background())
	if rep1.Get("flaky").Status != StatusFailed || rep1.Get("down").Status != StatusCanceled {
		t.Fatalf("判定依据=首次 flaky 应失败、down 应被阻塞取消, 实际=%s/%s",
			rep1.Get("flaky").Status, rep1.Get("down").Status)
	}
	if baseCalls != 1 || failCalls != 1 || downCalls != 0 {
		t.Fatalf("判定依据=首次调用计数异常 base=%d flaky=%d down=%d", baseCalls, failCalls, downCalls)
	}

	// 修复失败任务后重跑：base 命中缓存不重算，flaky 重跑成功，down 得以首次执行。
	shouldFail.Store(false)
	rep2, _ := r.Run(context.Background())
	for _, n := range []string{"base", "flaky", "down"} {
		if !rep2.Get(n).Status.OK() {
			t.Fatalf("task=<%s> 判定依据=恢复后应成功, status=%s err=%v",
				n, rep2.Get(n).Status, rep2.Get(n).Err)
		}
	}
	if baseCalls != 1 {
		t.Fatalf("task=<base> 判定依据=已成功且未失效的任务不该重算, 调用=%d", baseCalls)
	}
	if failCalls != 2 || downCalls != 1 {
		t.Fatalf("判定依据=flaky 应重跑、down 应首跑, flaky=%d down=%d", failCalls, downCalls)
	}
	t.Logf("判定依据=干净继续: base 缓存(调用%d) flaky 重跑成功(调用%d) down 首跑(调用%d)",
		baseCalls, failCalls, downCalls)
}

// 无关分支的结论不能被失败分支带偏。
func TestRun_FailureIsBranchIsolated(t *testing.T) {
	store := NewMemoryStore()
	tasks := []Task{
		{Name: "good", Input: []byte("g"), Fn: okFn},
		{Name: "bad", Input: []byte("b"), Fn: func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
			return nil, errSentinelKaboom
		}},
	}
	r, _ := New(tasks, store, Options{MaxConcurrency: 4})
	rep, _ := r.Run(context.Background())
	if rep.Get("good").Status != StatusSuccess {
		t.Fatalf("task=<good> 判定依据=无关分支应成功, 实际=%s err=%v",
			rep.Get("good").Status, rep.Get("good").Err)
	}
	if rep.Get("bad").Status != StatusFailed || !errors.Is(rep.Get("bad").Err, errSentinelKaboom) {
		t.Fatalf("task=<bad> 判定依据=应标记失败并保留原始错误, status=%s err=%v",
			rep.Get("bad").Status, rep.Get("bad").Err)
	}
	if len(rep.Failed()) != 1 || rep.Failed()[0].Name != "bad" {
		t.Fatalf("判定依据=失败列表应只含 bad, 实际=%v", rep.Failed())
	}
	t.Logf("task=<good> 判定依据=不受 bad 分支影响 status=%s", rep.Get("good").Status)
}

var errSentinelKaboom = errors.New("kaboom")
