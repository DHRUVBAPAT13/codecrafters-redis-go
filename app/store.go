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
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]Item),
		lists:   make(map[string][]string),
		streams: make(map[string]*Stream),
		blocked: make(map[string][]chan string),
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

	if item.isExpired() {
		s.mu.Lock()
		delete(s.data, key)
		s.mu.Unlock()
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
