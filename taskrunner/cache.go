package taskrunner

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"sync"
)

// MemoryStore 是并发安全的进程内缓存实现。
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]CacheEntry
}

// NewMemoryStore 创建一个空的进程内缓存。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string]CacheEntry)}
}

// Len 返回当前缓存条目数。
func (s *MemoryStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Get 实现 CacheStore。
func (s *MemoryStore) Get(key string) (CacheEntry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[key]
	return e, ok
}

// Put 实现 CacheStore。
func (s *MemoryStore) Put(key string, entry CacheEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[key] = entry
}

// Delete 实现 CacheStore。
func (s *MemoryStore) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
}

// ComputeKey 按「任务名 + 输入 + 依赖结果标识」计算结果标识。
//
// 任一字节变化（输入变化或任一依赖结果 Key 变化）都会产生不同标识。
// 依赖按名称排序后写入，保证计算结果与声明顺序无关、可复现。
// 每个字段以 8 字节小端长度作为前缀，避免字段拼接产生歧义。
func ComputeKey(name string, input []byte, depKeys map[string]string) string {
	h := sha256.New()
	h.Write([]byte("taskrunner/result-key/v1\n"))
	writeLenPrefixed(h, []byte("name:"+name))
	writeLenPrefixed(h, []byte("input"))
	writeLenPrefixed(h, input)

	names := make([]string, 0, len(depKeys))
	for n := range depKeys {
		names = append(names, n)
	}
	sort.Strings(names)
	var nbuf [8]byte
	binary.LittleEndian.PutUint64(nbuf[:], uint64(len(names)))
	h.Write(nbuf[:])
	for _, n := range names {
		writeLenPrefixed(h, []byte(n))
		writeLenPrefixed(h, []byte(depKeys[n]))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeLenPrefixed(h interface{ Write([]byte) (int, error) }, b []byte) {
	var nbuf [8]byte
	binary.LittleEndian.PutUint64(nbuf[:], uint64(len(b)))
	h.Write(nbuf[:])
	h.Write(b)
}
