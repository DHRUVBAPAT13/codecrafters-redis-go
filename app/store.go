package main

import (
	"errors"
	"strconv"
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
