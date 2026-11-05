package main

import (
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
