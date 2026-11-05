package main

import (
	"errors"
	"strconv"
	"strings"
)

type StreamEntry struct {
	ID     string
	Ms     int64
	Seq    int64
	Fields map[string]string
}

type Stream struct {
	Entries []StreamEntry
}

// parseStreamID splits and converts "<ms>-<seq>" into numerical values
func parseStreamID(id string) (int64, int64, error) {

	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid ID format")
	}

	ms, err1 := strconv.ParseInt(parts[0], 10, 64)
	seq, err2 := strconv.ParseInt(parts[1], 10, 64)

	if err1 != nil || err2 != nil {
		return 0, 0, errors.New("invalid integer in ID")
	}

	return ms, seq, nil
}

// XADD appends an entry to the stream and returns the entry ID
func (s *Store) XAdd(key, id string, fields map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ms, seq, err := parseStreamID(id)
	if err != nil {
		return "", errors.New("ERR invalid stream ID specified as stream command argument")
	}

	// 1. minimum ID rule
	if ms == 0 && seq == 0 {
		return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
	}

	stream, exits := s.streams[key]
	if !exits {
		stream = &Stream{
			Entries: make([]StreamEntry, 0),
		}
		s.streams[key] = stream
	}

	// 2. incremental comparison against the top item
	if len(stream.Entries) > 0 {
		lastEntry := stream.Entries[len(stream.Entries)-1]

		if ms < lastEntry.Ms || (ms == lastEntry.Ms && seq <= lastEntry.Seq) {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}

	stream.Entries = append(stream.Entries, StreamEntry{
		ID:     id,
		Ms:     ms,
		Seq:    seq,
		Fields: fields,
	})

	return id, nil
}
