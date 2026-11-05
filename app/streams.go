package main

type StreamEntry struct {
	ID     string
	Fields map[string]string
}

type Stream struct {
	Entries []StreamEntry
}

// XADD appends an entry to the stream and returns the entry ID
func (s *Store) XAdd(key, id string, fields map[string]string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	stream, exists := s.streams[key]
	if !exists {
		stream = &Stream{
			Entries: make([]StreamEntry, 0),
		}
		s.streams[key] = stream
	}

	stream.Entries = append(stream.Entries, StreamEntry{
		ID:     id,
		Fields: fields,
	})

	return id

}
