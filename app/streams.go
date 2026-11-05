package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
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

// XAdd validates, auto-generates IDs if necessary, and appends an entry to the stream
func (s *Store) XAdd(key, id string, fields map[string]string) (string, error) {

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

		// Case 1: Full auto-generation (*)
		nowMs := time.Now().UnixMilli()
		if len(stream.Entries) > 0 {
			lastEntry := stream.Entries[len(stream.Entries)-1]

			if nowMs == lastEntry.Ms {
				ms = lastEntry.Ms
				seq = lastEntry.Seq + 1
			} else if nowMs < lastEntry.Ms {
				// Prevent wall-clock backwards jump from breaking monotonicity
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
		// Case 2 & 3: Partial auto-generation (<ms>-*) or Explicit ID (<ms>-<seq>)
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

			// 0-0 is explicitly invalid
			if ms == 0 && seq == 0 {
				return "", errors.New("ERR The ID specified in XADD must be greater than 0-0")
			}

			// Strictly greater check
			if len(stream.Entries) > 0 {
				lastEntry := stream.Entries[len(stream.Entries)-1]
				if ms < lastEntry.Ms || (ms == lastEntry.Ms && seq <= lastEntry.Seq) {
					return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
				}
			}
		}
	}

	finalID := fmt.Sprintf("%d-%d", ms, seq)

	// Ensure Ms and Seq are explicitly saved on the entry
	stream.Entries = append(stream.Entries, StreamEntry{
		ID:     finalID,
		Ms:     ms,
		Seq:    seq,
		Fields: fields,
	})

	return finalID, nil
}
