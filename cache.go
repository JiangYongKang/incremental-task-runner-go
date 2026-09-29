package runner

import "sync"

// CacheEntry 是一条任务结果缓存：输入指纹、依赖结果指纹、结果指纹与结果值。
// 只有当输入与依赖结果指纹都与当前一致时，该条目才可能被命中。
type CacheEntry struct {
	InputFP  fingerprint
	DepsFP   fingerprint
	ResultFP fingerprint
	Value    []byte
}

// Cache 是任务结果缓存的存储抽象，实现需保证并发安全。
type Cache interface {
	Get(id TaskID) (CacheEntry, bool)
	Put(id TaskID, entry CacheEntry)
}

// MemoryCache 是进程内、并发安全的缓存实现，可在多次 Run 之间复用，
// 因此重新执行 Runner 时已完成且未失效的任务不会被重算。
type MemoryCache struct {
	mu sync.Mutex
	m  map[TaskID]CacheEntry
}

// NewMemoryCache 创建一个空的 MemoryCache。
func NewMemoryCache() *MemoryCache {
	return &MemoryCache{m: make(map[TaskID]CacheEntry)}
}

func (c *MemoryCache) Get(id TaskID) (CacheEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[id]
	if ok {
		// 返回值副本，避免调用方修改缓存内部数据。
		v := make([]byte, len(e.Value))
		copy(v, e.Value)
		e.Value = v
	}
	return e, ok
}

func (c *MemoryCache) Put(id TaskID, entry CacheEntry) {
	v := make([]byte, len(entry.Value))
	copy(v, entry.Value)
	entry.Value = v
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[id] = entry
}
