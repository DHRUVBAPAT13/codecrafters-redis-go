package main

import "time"

func (s *Store) RPush(key string, values ...string) int {
	s.mu.Lock()

	s.lists[key] = append(s.lists[key], values...)
	newLen := len(s.lists[key])

	type notification struct {
		ch  chan string
		val string
	}
	var toNotify []notification

	for len(s.lists[key]) > 0 && len(s.blocked[key]) > 0 {
		ch := s.blocked[key][0]
		s.blocked[key] = s.blocked[key][1:]
		if len(s.blocked[key]) == 0 {
			delete(s.blocked, key)
		}

		val := s.lists[key][0]
		s.lists[key] = s.lists[key][1:]
		if len(s.lists[key]) == 0 {
			delete(s.lists, key)
		}

		toNotify = append(toNotify, notification{ch: ch, val: val})
	}
	s.mu.Unlock()

	for _, n := range toNotify {
		n.ch <- n.val
	}

	return newLen
}

func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()

	reversed := make([]string, len(values))
	for i, v := range values {
		reversed[len(values)-1-i] = v
	}
	s.lists[key] = append(reversed, s.lists[key]...)
	newLen := len(s.lists[key])

	type notification struct {
		ch  chan string
		val string
	}
	var toNotify []notification

	for len(s.lists[key]) > 0 && len(s.blocked[key]) > 0 {
		ch := s.blocked[key][0]
		s.blocked[key] = s.blocked[key][1:]
		if len(s.blocked[key]) == 0 {
			delete(s.blocked, key)
		}

		val := s.lists[key][0]
		s.lists[key] = s.lists[key][1:]
		if len(s.lists[key]) == 0 {
			delete(s.lists, key)
		}

		toNotify = append(toNotify, notification{ch: ch, val: val})
	}
	s.mu.Unlock()

	for _, n := range toNotify {
		n.ch <- n.val
	}

	return newLen
}

func (s *Store) LRange(key string, start, stop int) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return []string{}
	}

	n := len(list)

	if start < 0 {
		start = n + start
		if start < 0 {
			start = 0
		}
	}

	if stop < 0 {
		stop = n + stop
		if stop < 0 {
			stop = 0
		}
	}

	if start >= n || start > stop {
		return []string{}
	}

	if stop >= n {
		stop = n - 1
	}

	result := make([]string, stop-start+1)
	copy(result, list[start:stop+1])
	return result
}

func (s *Store) LLen(key string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	list, ok := s.lists[key]
	if !ok {
		return 0
	}
	return len(list)
}

func (s *Store) LPop(key string, count int) ([]string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return nil, false
	}

	if count > len(list) {
		count = len(list)
	}

	popped := make([]string, count)
	copy(popped, list[:count])

	s.lists[key] = list[count:]
	if len(s.lists[key]) == 0 {
		delete(s.lists, key)
	}

	return popped, true
}

func (s *Store) BLPop(key string, timeout time.Duration) (string, bool) {
	s.mu.Lock()

	if list, ok := s.lists[key]; ok && len(list) > 0 {
		val := list[0]
		s.lists[key] = list[1:]
		if len(s.lists[key]) == 0 {
			delete(s.lists, key)
		}
		s.mu.Unlock()
		return val, true
	}

	ch := make(chan string, 1)
	s.blocked[key] = append(s.blocked[key], ch)
	s.mu.Unlock()

	if timeout == 0 {
		val := <-ch
		return val, true
	}

	select {
	case val := <-ch:
		return val, true
	case <-time.After(timeout):
		s.mu.Lock()
		waiters := s.blocked[key]
		for i, waiter := range waiters {
			if waiter == ch {
				s.blocked[key] = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
		if len(s.blocked[key]) == 0 {
			delete(s.blocked, key)
		}
		s.mu.Unlock()

		select {
		case val := <-ch:
			return val, true
		default:
			return "", false
		}
	}
}
