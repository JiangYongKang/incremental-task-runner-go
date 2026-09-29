package runner

import (
	"context"
	"time"
)

// TaskID 是任务的唯一标识。
type TaskID string

// Status 表示任务在一次调度执行中的状态。
type Status int

const (
	StatusPending Status = iota
	StatusRunning
	StatusSucceeded
	StatusFailed
	StatusTimedOut
	StatusCanceled
	StatusUpstreamFailed
)

// String 返回状态的可读名称。
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusSucceeded:
		return "succeeded"
	case StatusFailed:
		return "failed"
	case StatusTimedOut:
		return "timeout"
	case StatusCanceled:
		return "canceled"
	case StatusUpstreamFailed:
		return "upstream_failed"
	default:
		return "unknown"
	}
}

// TaskFunc 是任务的实际执行逻辑。
type TaskFunc func(ctx context.Context, in TaskInput) (TaskOutput, error)

// Task 声明一个任务及其依赖。
type Task struct {
	ID      TaskID
	Input   []byte
	Deps    []TaskID
	Timeout time.Duration
	Run     TaskFunc
}

// TaskInput 是传给 TaskFunc 的输入，Deps 为依赖任务的结果值（按依赖 ID 索引）。
type TaskInput struct {
	ID    TaskID
	Input []byte
	Deps  map[TaskID][]byte
}

// TaskOutput 是任务执行成功后的结果。
type TaskOutput struct {
	Value []byte
}
