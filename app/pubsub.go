package main

import (
	"net"
	"sync"
)

type PubSubMangager struct {
	mu       sync.RWMutex
	channels map[string]map[net.Conn]struct{}
}

var pubsubManager = &PubSubMangager{
	channels: make(map[string]map[net.Conn]struct{}),
}

// subscribe adds a connection to a specific channel
func (ps *PubSubMangager) Subscribe(channel string, conn net.Conn) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if _, ok := ps.channels[channel]; !ok {
		ps.channels[channel] = make(map[net.Conn]struct{})
	}
	ps.channels[channel][conn] = struct{}{}
}

// UnsubscribeAll removes a connection from all channels when it disconnects
func (ps *PubSubMangager) UnsubscribeAll(conn net.Conn) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for channel, subscribers := range ps.channels {
		delete(subscribers, conn)
		if len(subscribers) == 0 {
			delete(ps.channels, channel)
		}
	}
}

// SubscriberCount returns the number of subscribers for a given channel
func (ps *PubSubMangager) SubscriberCount(channel string) int {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return len(ps.channels[channel])
}
