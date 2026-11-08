package main

import (
	"fmt"
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

// Publish delivers a message to all subscribers of a given channel
func (ps *PubSubMangager) Publish(channel string, message string) int {
	ps.mu.RLock()

	subscribersMap, exists := ps.channels[channel]
	if !exists || len(subscribersMap) == 0 {
		ps.mu.RUnlock()
		return 0
	}

	// make a snapshot of subscribers to release the lock while writing to the network
	subscribers := make([]net.Conn, 0, len(subscribersMap))
	for conn := range subscribersMap {
		subscribers = append(subscribers, conn)
	}
	ps.mu.RUnlock()

	// format : *3\r\n$7\r\nmessage\r\n$len(channel)\r\nchannel\r\n$len(message)\r\nmsg\r\n
	payload := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channel), channel, len(message), message)
	payloadBytes := []byte(payload)

	for _, conn := range subscribers {
		_, _ = conn.Write([]byte(payloadBytes))
	}

	return len(subscribers)
}
