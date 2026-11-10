package main

import (
	"errors"
	"math/bits"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Item struct {
	value     string
	expiresAt time.Time
}

func (item Item) isExpired() bool {
	if item.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(item.expiresAt)
}

type Store struct {
	mu      sync.RWMutex
	data    map[string]Item
	lists   map[string][]string
	streams map[string]*Stream
	blocked map[string][]chan string
	zsets   map[string]*ZSet
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]Item),
		lists:   make(map[string][]string),
		streams: make(map[string]*Stream),
		blocked: make(map[string][]chan string),
		zsets:   make(map[string]*ZSet),
	}
}

func (s *Store) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	s.data[key] = Item{
		value:     value,
		expiresAt: expiresAt,
	}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	item, ok := s.data[key]
	s.mu.RUnlock()

	if !ok {
		return "", false
	}

	// Only expire if expiresAt was actually set!
	if !item.expiresAt.IsZero() && time.Now().After(item.expiresAt) {
		return "", false
	}

	return item.value, true
}

func (s *Store) Type(key string) string {

	s.mu.RLock()
	defer s.mu.RUnlock()

	// check string data
	if item, ok := s.data[key]; ok {
		if !item.isExpired() {
			return "string"
		}
	}

	// check list data
	if list, ok := s.lists[key]; ok && len(list) > 0 {
		return "list"
	}

	if stream, ok := s.streams[key]; ok && len(stream.Entries) > 0 {
		return "stream"
	}

	if zs, ok := s.zsets[key]; ok && zs.sl.length > 0 {
		return "zset"
	}

	return "none"
}

func (s *Store) Incr(key string) (int64, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	item, ok := s.data[key]
	if !ok || item.isExpired() {
		// if key does not exist or is expired, initialize with 1
		s.data[key] = Item{
			value: "1",
		}
		return 1, nil
	}

	val, err := strconv.ParseInt(item.value, 10, 64)
	if err != nil {
		return 0, errors.New("ERR value is not an integer or out of range")
	}

	val++
	item.value = strconv.FormatInt(val, 10)
	s.data[key] = item

	return val, nil
}

func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]string, 0, len(s.data))
	now := time.Now()

	for k, item := range s.data {
		if !item.expiresAt.IsZero() && now.After(item.expiresAt) {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) ZAdd(key string, score float64, member string) int {

	s.mu.Lock()
	defer s.mu.Unlock()

	zs, exists := s.zsets[key]
	if !exists {
		zs = NewZSet()
		s.zsets[key] = zs
	}

	return zs.Add(score, member)
}

func (s *Store) ZRank(key, member string) (int, bool) {

	s.mu.Lock()
	defer s.mu.Unlock()

	zs, exists := s.zsets[key]
	if !exists {
		return -1, false
	}

	return zs.Rank(member)
}

func (s *Store) ZRange(key string, start, stop int) []string {

	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, exists := s.zsets[key]
	if !exists {
		return []string{}
	}

	return zs.Range(start, stop)
}

func (s *Store) ZCard(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, exists := s.zsets[key]
	if !exists {
		return 0
	}

	return zs.Card()
}

func (s *Store) ZScore(key, member string) (float64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, exists := s.zsets[key]
	if !exists {
		return 0, false
	}

	return zs.Score(member)
}

func (s *Store) ZRem(key string, members ...string) int {

	s.mu.Lock()
	defer s.mu.Unlock()

	zs, exists := s.zsets[key]
	if !exists {
		return 0
	}

	removed := 0
	for _, member := range members {
		removed += zs.Rem(member)
	}

	return removed
}

func (s *Store) SetBit(key string, offset int, bit int) (int, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	item, exists := s.data[key]
	var bytes []byte

	if exists && !item.isExpired() {
		bytes = []byte(item.value)
	}

	byteIndex := offset / 8
	bitOffset := 7 - (offset % 8)
	mask := byte(1 << bitOffset)

	originalBit := 0

	// expnad the byte slice if the target offset exceeds current capacity
	if byteIndex >= len(bytes) {
		newBytes := make([]byte, byteIndex+1)
		copy(newBytes, bytes)
		bytes = newBytes
	} else {
		if (bytes[byteIndex] & mask) != 0 {
			originalBit = 1
		}
	}

	if bit == 1 {
		bytes[byteIndex] |= mask
	} else {
		bytes[byteIndex] &= ^mask
	}

	var expiresAt time.Time
	if exists && !item.isExpired() {
		expiresAt = item.expiresAt
	}

	s.data[key] = Item{
		value:     string(bytes),
		expiresAt: expiresAt,
	}

	return originalBit, nil
}

func (s *Store) GetBit(key string, offset int) int {

	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.data[key]
	if !exists || item.isExpired() {
		return 0
	}

	byteIndex := offset / 8
	if byteIndex >= len(item.value) {
		return 0
	}

	bitOffset := 7 - (offset % 8)
	mask := byte(1 << bitOffset)

	if (item.value[byteIndex] & mask) != 0 {
		return 1
	}

	return 0
}

func (s *Store) StrLen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.data[key]
	if !exists || item.isExpired() {
		return 0
	}

	return len(item.value)
}

func (s *Store) BitCount(key string, hasRange bool, start, end int) int {

	s.mu.RLock()
	defer s.mu.RUnlock()

	item, exists := s.data[key]
	if !exists {
		return 0
	}

	valBytes := []byte(item.value)
	n := len(valBytes)
	if n == 0 {
		return 0
	}

	if !hasRange {
		start = 0
		end = n - 1
	} else {
		if start < 0 {
			start = 0
		}
		if start >= n || start > end {
			return 0
		}
		if end >= n {
			end = n - 1
		}
	}

	count := 0
	for i := start; i <= end; i++ {
		count += bits.OnesCount8(valBytes[i])
	}

	return count
}

func (s *Store) BitOp(op, destKey string, srcKeys ...string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	op = strings.ToUpper(op)
	if op != "AND" && op != "OR" {
		return 0, errors.New("ERR syntax error")
	}

	if len(srcKeys) == 0 {
		return 0, nil
	}

	// retrieve byte slices for all source keys
	srcs := make([][]byte, len(srcKeys))
	maxLen := 0

	for i, key := range srcKeys {
		if item, exists := s.data[key]; exists && !item.isExpired() {
			srcs[i] = []byte(item.value)
			if len(srcs[i]) > maxLen {
				maxLen = len(srcs)
			}
		} else {
			srcs[i] = []byte{}
		}
	}

	if maxLen == 0 {
		delete(s.data, destKey)
		return 0, nil
	}

	destBytes := make([]byte, maxLen)

	switch op {
	case "AND":
		// initialze with first source bytes
		copy(destBytes, srcs[0])

		// any missing bytes past the first source length remain 0x00
		for j := 1; j < len(srcs); j++ {
			for i := 0; i < maxLen; i++ {
				var b byte

				if i < len(srcs[j]) {
					b = srcs[j][i]
				}
				destBytes[i] &= b
			}
		}
	case "OR":
		for _, src := range srcs {
			for i := 0; i < len(src); i++ {
				destBytes[i] |= src[i]
			}
		}
	}

	s.data[destKey] = Item{
		value:     string(destBytes),
		expiresAt: time.Time{},
	}

	return len(destBytes), nil
}

func (s *Store) GetAllZSetMembers(key string) map[string]float64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	zs, exists := s.zsets[key]
	if !exists {
		return nil
	}

	result := make(map[string]float64)
	zs.dict.Range(func(key, value any) bool {
		result[key.(string)] = value.(float64)
		return true
	})

	return result
}
