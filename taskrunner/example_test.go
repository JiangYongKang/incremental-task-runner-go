package taskrunner_test

import (
	"context"
	"errors"
	"fmt"

	tr "github.com/highcumontoa/incremental-task-runner-go/taskrunner"
)

func Example() {
	store := tr.NewMemoryStore()
	tasks := []tr.Task{
		{
			Name:  "compile",
			Input: []byte("source-v1"),
			Fn: func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
				return append([]byte("artifact:"), in...), nil
			},
		},
		{
			Name:  "test",
			Deps:  []string{"compile"},
			Input: []byte("suite-v1"),
			Fn: func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
				return append(deps["compile"], []byte("+tested")...), nil
			},
		},
	}

	r, err := tr.New(tasks, store, tr.Options{MaxConcurrency: 4})
	if err != nil {
		panic(err)
	}

	// 第一次：真实执行。
	rep1, _ := r.Run(context.Background())
	for _, name := range []string{"compile", "test"} {
		fmt.Printf("run1 %s: %s -> %s\n", name, rep1.Get(name).Status, rep1.Get(name).Output)
	}

	// 第二次：输入未变，全部命中缓存，Fn 不会被调用，但对外仍是成功结论。
	rep2, _ := r.Run(context.Background())
	for _, name := range []string{"compile", "test"} {
		fmt.Printf("run2 %s: %s -> %s\n", name, rep2.Get(name).Status, rep2.Get(name).Output)
	}

	// 声明非法时拿到可区分的原因。
	_, err = tr.New([]tr.Task{{Name: "x", Deps: []string{"x"}}}, store, tr.Options{})
	fmt.Println("invalid:", errors.Is(err, tr.ErrDependencyCycle))

	// Output:
	// run1 compile: success -> artifact:source-v1
	// run1 test: success -> artifact:source-v1+tested
	// run2 compile: cached -> artifact:source-v1
	// run2 test: cached -> artifact:source-v1+tested
	// invalid: true
}
