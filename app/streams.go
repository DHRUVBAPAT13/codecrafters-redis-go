package main

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StreamField struct {
	Key   string
	Value string
}

type StreamEntry struct {
	ID     string
	Ms     int64
	Seq    int64
	Fields []StreamField
}

type Stream struct {
	Entries []StreamEntry
}

// track listeners waiting on new stream entries
type StreamWaiter struct {
	key string
	ch  chan string
}

var (
	streamWaitersMu sync.Mutex
	streamWaiters   = make(map[string][]chan struct{})
)

func registerStreamWaiter(key string) chan struct{} {

	streamWaitersMu.Lock()
	defer streamWaitersMu.Unlock()

	ch := make(chan struct{}, 1)
	streamWaiters[key] = append(streamWaiters[key], ch)

	return ch
}

func removeStreamWaiter(key string, ch chan struct{}) {

	streamWaitersMu.Lock()
	defer streamWaitersMu.Unlock()

	waiters := streamWaiters[key]
	for i, w := range waiters {
		if w == ch {
			streamWaiters[key] = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}

	if len(streamWaiters[key]) == 0 {
		delete(streamWaiters, key)
	}
}

func notifyStreamWiater(key string) {

	streamWaitersMu.Lock()
	defer streamWaitersMu.Unlock()

	for _, ch := range streamWaiters[key] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	delete(streamWaiters, key)
}

// parseStreamID handles explicit IDs ("<ms>-<seq>") and partial auto-generation ("<ms>-*")
func parseStreamID(id string) (ms int64, seq int64, isAutoSeq bool, err error) {

	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, false, errors.New("invalid id format")
	}

	ms, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ms < 0 {
		return 0, 0, false, errors.New("invalid milliseconds time")
	}

	if parts[1] == "*" {
		return ms, 0, true, nil
	}

	seq, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || seq < 0 {
		return 0, 0, false, errors.New("invalid sequence number")
	}

	return ms, seq, false, nil
}

// parseRangeBound parses start and end bounds for XRANGE including optional sequence numbers and - / +
func parseRangeBound(bound string, isEnd bool) (int64, int64, error) {
	if bound == "-" {
		return 0, 0, nil
	}
	if bound == "+" {
		return math.MaxInt64, math.MaxInt64, nil
	}

	parts := strings.Split(bound, "-")
	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ms < 0 {
		return 0, 0, errors.New("invalid range bound")
	}

	if len(parts) == 1 {
		if isEnd {
			return ms, math.MaxInt64, nil
		}
		return ms, 0, nil
	}

	seq, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || seq < 0 {
		return 0, 0, errors.New("invalid sequence number in bound")
	}

	return ms, seq, nil
}

func compareStreamIDs(ms1, seq1, ms2, seq2 int64) int {
	if ms1 < ms2 {
		return -1
	}
	if ms1 > ms2 {
		return 1
	}
	if seq1 < seq2 {
		return -1
	}
	if seq1 > seq2 {
		return 1
	}
	return 0
}

// XAdd validates, auto-generates IDs if necessary, and appends an entry to the stream
func (s *Store) XAdd(key, id string, fields []StreamField) (string, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	stream, exists := s.streams[key]
	if !exists {
		stream = &Stream{
			Entries: make([]StreamEntry, 0),
		}
		s.streams[key] = stream
	}

	var ms, seq int64

	if id == "*" {
		nowMs := time.Now().UnixMilli()
		if len(stream.Entries) > 0 {
			lastEntry := stream.Entries[len(stream.Entries)-1]
			if nowMs <= lastEntry.Ms {
				ms = lastEntry.Ms
				seq = lastEntry.Seq + 1
			} else {
				ms = nowMs
				seq = 0
			}
		} else {
			ms = nowMs
			seq = 0
		}
	} else {
		parsedMs, parsedSeq, isAutoSeq, err := parseStreamID(id)
		if err != nil {
			return "", errors.New("ERR invalid stream ID specified as stream command argument")
		}

		ms = parsedMs

		if isAutoSeq {
			if len(stream.Entries) > 0 {
				lastEntry := stream.Entries[len(stream.Entries)-1]
				if ms < lastEntry.Ms {
					return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
				}
				if ms == lastEntry.Ms {
					seq = lastEntry.Seq + 1
				} else {
					if ms == 0 {
						seq = 1
					} else {
						seq = 0
					}
				}
			} else {
				if ms == 0 {
					seq = 1
				} else {
					seq = 0
				}
			}
		} else {
			seq = parsedSeq

			if ms == 0 && seq == 0 {
				return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
			}

			if len(stream.Entries) > 0 {
				lastEntry := stream.Entries[len(stream.Entries)-1]
				if ms < lastEntry.Ms || (ms == lastEntry.Ms && seq <= lastEntry.Seq) {
					return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
				}
			}
		}
	}

	finalID := fmt.Sprintf("%d-%d", ms, seq)

	stream.Entries = append(stream.Entries, StreamEntry{
		ID:     finalID,
		Ms:     ms,
		Seq:    seq,
		Fields: fields,
	})

	// notify any blocked XREAD clients
	go notifyStreamWiater(key)

	return finalID, nil
}

// XRange returns entries within the inclusive [start, end] boundary
func (s *Store) XRange(key, startBound, endBound string) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, exists := s.streams[key]
	if !exists || len(stream.Entries) == 0 {
		return []StreamEntry{}, nil
	}

	startMs, startSeq, err := parseRangeBound(startBound, false)
	if err != nil {
		return nil, errors.New("ERR invalid start bound")
	}

	endMs, endSeq, err := parseRangeBound(endBound, true)
	if err != nil {
		return nil, errors.New("ERR invalid end bound")
	}

	var results []StreamEntry
	for _, entry := range stream.Entries {
		if compareStreamIDs(entry.Ms, entry.Seq, startMs, startSeq) >= 0 &&
			compareStreamIDs(entry.Ms, entry.Seq, endMs, endSeq) <= 0 {
			results = append(results, entry)
		}
	}

	return results, nil
}

func (s *Store) XRead(key, afterID string) ([]StreamEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, exists := s.streams[key]
	if !exists || len(stream.Entries) == 0 {
		return []StreamEntry{}, nil
	}

	targetMs, targetSeq, _, err := parseStreamID(afterID)
	if err != nil {
		return nil, errors.New("ERR invalid stream ID specified as stream command argument")
	}

	var results []StreamEntry
	for _, entry := range stream.Entries {
		// strict inequality: entry > targetID
		if compareStreamIDs(entry.Ms, entry.Seq, targetMs, targetSeq) > 0 {
			results = append(results, entry)
		}
	}

	return results, nil
}

// GetLastID returns the ID of the last entry in the stream, or "0-0" if empty
func (s *Store) GetLastID(key string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stream, exists := s.streams[key]
	if !exists || len(stream.Entries) == 0 {
		return "0-0"
	}

	return stream.Entries[len(stream.Entries)-1].ID
}
