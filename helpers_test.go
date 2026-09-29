package runner_test

import (
	"fmt"
	"sync"
	"testing"
)

// testLogger 把判定日志写入测试输出，日志包含任务标识与判定依据。
type testLogger struct {
	mu sync.Mutex
	t  *testing.T
}

func newTestLogger(t *testing.T) *testLogger { return &testLogger{t: t} }

func (l *testLogger) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.t.Logf("[runner] %s", fmt.Sprintf(format, args...))
}
