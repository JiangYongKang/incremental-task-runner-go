package runner

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
)

// fingerprint 是基于内容计算出的稳定标识。
type fingerprint string

// hashBytes 将若干段内容以“长度 + 数据”的方式拼接后计算 SHA-256 指纹，
// 长度前缀保证不同分段方式不会产生碰撞。
func hashBytes(parts ...[]byte) fingerprint {
	h := sha256.New()
	var lenBuf [8]byte
	for _, p := range parts {
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(p)))
		h.Write(lenBuf[:])
		h.Write(p)
	}
	return fingerprint(hex.EncodeToString(h.Sum(nil)))
}

// hashDepResults 按依赖 ID 排序后对 (ID, 结果指纹) 序列求哈希，
// 使依赖结果指纹只取决于内容而与 map 遍历顺序无关。
func hashDepResults(results map[TaskID]fingerprint) fingerprint {
	ids := make([]TaskID, 0, len(results))
	for id := range results {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	parts := make([][]byte, 0, len(ids)*2)
	for _, id := range ids {
		parts = append(parts, []byte(id), []byte(results[id]))
	}
	return hashBytes(parts...)
}
