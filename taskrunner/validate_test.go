package taskrunner

import (
	"errors"
	"testing"
)

func TestValidate_DuplicateTask(t *testing.T) {
	tasks := []Task{
		{Name: "a", Fn: okFn},
		{Name: "a", Fn: okFn},
	}
	_, err := New(tasks, NewMemoryStore(), Options{MaxConcurrency: 2})
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("task=<%s> 判定依据=期望 ErrDuplicateTask, 实际=%v", "a", err)
	}
	var de *DuplicateTaskError
	if !errors.As(err, &de) || de.Name != "a" {
		t.Fatalf("task=<%s> 判定依据=错误详情未携带任务名: %v", "a", err)
	}
	t.Logf("task=<%s> 判定依据=重复声明被拒绝: %v", "a", err)
}

func TestValidate_DependencyMissing(t *testing.T) {
	tasks := []Task{
		{Name: "a", Deps: []string{"ghost"}, Fn: okFn},
	}
	_, err := New(tasks, NewMemoryStore(), Options{})
	if !errors.Is(err, ErrDependencyMissing) {
		t.Fatalf("task=<a> 判定依据=期望 ErrDependencyMissing, 实际=%v", err)
	}
	var me *DependencyMissingError
	if !errors.As(err, &me) || me.Task != "a" || me.Dependency != "ghost" {
		t.Fatalf("task=<a> 判定依据=错误未携带任务/依赖名: %v", err)
	}
	t.Logf("task=<a dep=ghost> 判定依据=依赖缺失被拒绝: %v", err)
}

func TestValidate_DependencyCycle(t *testing.T) {
	cases := []struct {
		name  string
		tasks []Task
	}{
		{
			name: "self-loop",
			tasks: []Task{
				{Name: "a", Deps: []string{"a"}, Fn: okFn},
			},
		},
		{
			name: "three-node-loop",
			tasks: []Task{
				{Name: "a", Deps: []string{"c"}, Fn: okFn},
				{Name: "b", Deps: []string{"a"}, Fn: okFn},
				{Name: "c", Deps: []string{"b"}, Fn: okFn},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.tasks, NewMemoryStore(), Options{})
			if !errors.Is(err, ErrDependencyCycle) {
				t.Fatalf("case=<%s> 判定依据=期望 ErrDependencyCycle, 实际=%v", tc.name, err)
			}
			var ce *DependencyCycleError
			if !errors.As(err, &ce) || len(ce.Cycle) < 2 {
				t.Fatalf("case=<%s> 判定依据=错误未携带环路径: %v", tc.name, err)
			}
			t.Logf("case=<%s> 判定依据=依赖成环被拒绝, 环=%v", tc.name, ce.Cycle)
		})
	}
}

func TestValidate_EmptyName(t *testing.T) {
	_, err := New([]Task{{Name: "", Fn: okFn}}, NewMemoryStore(), Options{})
	if !errors.Is(err, ErrEmptyName) {
		t.Fatalf("判定依据=期望 ErrEmptyName, 实际=%v", err)
	}
	t.Logf("判定依据=空任务名被拒绝: %v", err)
}
