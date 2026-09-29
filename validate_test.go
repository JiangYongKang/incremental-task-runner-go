package runner_test

import (
	"context"
	"errors"
	"testing"

	runner "github.com/highcumontoa/incremental-task-runner-go"
)

func okFunc(_ context.Context, in runner.TaskInput) (runner.TaskOutput, error) {
	return runner.TaskOutput{Value: []byte("out:" + string(in.Input))}, nil
}

// 依赖成环：必须以 *CycleError 拒绝，环上任务标识可读出。
func TestRejectCycle(t *testing.T) {
	tasks := []*runner.Task{
		{ID: "a", Deps: []runner.TaskID{"b"}, Run: okFunc},
		{ID: "b", Deps: []runner.TaskID{"c"}, Run: okFunc},
		{ID: "c", Deps: []runner.TaskID{"a"}, Run: okFunc},
	}
	_, err := runner.New(tasks)
	var ce *runner.CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CycleError, got %v", err)
	}
	t.Logf("判定依据: 检测到依赖环, cycle=%v", ce.Cycle)
	if len(ce.Cycle) < 2 {
		t.Fatalf("expected non-empty cycle, got %v", ce.Cycle)
	}
}

// 自依赖也成环。
func TestRejectSelfCycle(t *testing.T) {
	_, err := runner.New([]*runner.Task{
		{ID: "a", Deps: []runner.TaskID{"a"}, Run: okFunc},
	})
	var ce *runner.CycleError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *CycleError, got %v", err)
	}
	t.Logf("判定依据: 自依赖成环, cycle=%v", ce.Cycle)
}

// 重复声明：必须以 *DuplicateTaskError 拒绝。
func TestRejectDuplicate(t *testing.T) {
	_, err := runner.New([]*runner.Task{
		{ID: "a", Run: okFunc},
		{ID: "a", Run: okFunc},
	})
	var de *runner.DuplicateTaskError
	if !errors.As(err, &de) {
		t.Fatalf("expected *DuplicateTaskError, got %v", err)
	}
	if de.ID != "a" {
		t.Fatalf("expected duplicate id a, got %q", de.ID)
	}
	t.Logf("判定依据: 任务 a 被重复声明, rejected_id=%s", de.ID)
}

// 依赖缺失：必须以 *MissingDependencyError 拒绝，并能区分缺失的是哪个 ID。
func TestRejectMissingDependency(t *testing.T) {
	_, err := runner.New([]*runner.Task{
		{ID: "a", Deps: []runner.TaskID{"ghost"}, Run: okFunc},
	})
	var me *runner.MissingDependencyError
	if !errors.As(err, &me) {
		t.Fatalf("expected *MissingDependencyError, got %v", err)
	}
	if me.TaskID != "a" || me.Missing != "ghost" {
		t.Fatalf("unexpected missing dep error: %+v", me)
	}
	t.Logf("判定依据: 任务 %s 依赖了不存在的任务 %s", me.TaskID, me.Missing)
}

// 非法声明：空 ID、nil 执行函数。
func TestRejectInvalidTask(t *testing.T) {
	if _, err := runner.New([]*runner.Task{{ID: "", Run: okFunc}}); err == nil {
		t.Fatal("expected error for empty id")
	}
	if _, err := runner.New([]*runner.Task{{ID: "a", Run: nil}}); err == nil {
		t.Fatal("expected error for nil run func")
	}
	t.Log("判定依据: 空 ID 与 nil Run 均被拒绝")
}

// 并发上限必须为正数。
func TestRejectBadConcurrency(t *testing.T) {
	_, err := runner.New([]*runner.Task{{ID: "a", Run: okFunc}}, runner.WithConcurrency(0))
	if err == nil {
		t.Fatal("expected error for zero concurrency")
	}
	t.Logf("判定依据: 非法并发参数被拒绝, err=%v", err)
}

// 三种声明错误应可按类型相互区分。
func TestDeclarationErrorsAreDistinguishable(t *testing.T) {
	_, cycle := runner.New([]*runner.Task{
		{ID: "a", Deps: []runner.TaskID{"b"}, Run: okFunc},
		{ID: "b", Deps: []runner.TaskID{"a"}, Run: okFunc},
	})
	_, dup := runner.New([]*runner.Task{
		{ID: "a", Run: okFunc}, {ID: "a", Run: okFunc},
	})
	_, missing := runner.New([]*runner.Task{
		{ID: "a", Deps: []runner.TaskID{"x"}, Run: okFunc},
	})
	if !errors.As(cycle, new(*runner.CycleError)) ||
		!errors.As(dup, new(*runner.DuplicateTaskError)) ||
		!errors.As(missing, new(*runner.MissingDependencyError)) {
		t.Fatalf("error kinds not distinguished: cycle=%v dup=%v missing=%v", cycle, dup, missing)
	}
	t.Log("判定依据: 成环/重复/缺失分别对应不同错误类型")
}
