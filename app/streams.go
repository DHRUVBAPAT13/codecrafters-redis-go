package main

import (
	"errors"
	"fmt"
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
func parseStreamID(id string) (ms int64, seq int64, isAutoSeq bool, err error) {

	parts := strings.Split(id, "-")
	if len(parts) != 2 {
		return 0, 0, false, errors.New("invalid ID format")
	}

	ms, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ms < 0 {
		return 0, 0, false, errors.New("invalid millisecond time")
	}

	if parts[1] == "*" {
		return ms, 0, true, nil
	}

	seq, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || seq < 0 {
		return 0, 0, false, errors.New("invalid sequence number")
	}

	return ms, seq, isAutoSeq, err
}

// XADD appends an entry to the stream and returns the entry ID
func (s *Store) XAdd(key, id string, fields map[string]string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, exits := s.streams[key]
	if !exits {
		stream = &Stream{
			Entries: make([]StreamEntry, 0),
		}
		s.streams[key] = stream
	}

	ms, seq, isAutoSeq, err := parseStreamID(id)
	if err != nil {
		return "", errors.New("ERR invalid stream ID specified as stream command argument")
	}
	if isAutoSeq {
		if len(stream.Entries) > 0 {
			lastEntry := stream.Entries[len(stream.Entries)-1]
			if ms < lastEntry.Ms {
				return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
			}

			if ms == lastEntry.Ms {
				seq = lastEntry.Seq
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
		// 0-0 is invalid
		if ms == 0 && seq == 0 {
			return "", errors.New("ERR the specified ID in XADD most be greater than 0-0")
		}

		// must be strictly greater than the last entry
		if len(stream.Entries) > 0 {
			lastEntry := stream.Entries[len(stream.Entries)-1]
			if ms < lastEntry.Ms || (ms == lastEntry.Ms && seq <= lastEntry.Seq) {
				return "", errors.New("ERR the specified ID in XADD is equal or smaller than the target stream top item")
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

	return finalID, nil
}
