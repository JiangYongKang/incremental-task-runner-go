package runner

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Logger 用于输出调度判定日志，日志中应包含任务标识与判定依据。
type Logger interface {
	Logf(format string, args ...any)
}

// discardLogger 是默认日志器，不产生任何输出。
type discardLogger struct{}

func (discardLogger) Logf(string, ...any) {}

// writerLogger 将日志按行写入指定 io.Writer，并发安全。
type writerLogger struct {
	mu sync.Mutex
	w  io.Writer
}

// NewLogger 构造一个写入 w 的基础 Logger，每行带时间戳。
func NewLogger(w io.Writer) Logger {
	return &writerLogger{w: w}
}

func (l *writerLogger) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(l.w, "%s %s\n", time.Now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}
