package taskrunner

import (
	"context"
	"sync/atomic"
	"time"
)

var okFn TaskFunc = func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
	return append([]byte(nil), in...), nil
}

// countingFn 每被真实调用一次原子加一，并调用 hook（可为 nil）。
func countingFn(counter *int32, hook func()) TaskFunc {
	return func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
		atomic.AddInt32(counter, 1)
		if hook != nil {
			hook()
		}
		return append([]byte(nil), in...), nil
	}
}

// blockingFn 进入执行后阻塞，直到 ctx 取消或 release 被关闭。
func blockingFn(entered chan<- struct{}, release <-chan struct{}) TaskFunc {
	return func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return in, nil
		}
	}
}

// slowFn 至少等待 d；ctx 先取消则返回 ctx.Err。
func slowFn(d time.Duration) TaskFunc {
	return func(ctx context.Context, in []byte, deps map[string][]byte) ([]byte, error) {
		select {
		case <-time.After(d):
			return in, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}
