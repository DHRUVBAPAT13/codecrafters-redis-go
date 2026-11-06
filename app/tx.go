package main

import "sync"

type ClientTxState struct {
	InTx        bool
	Queue       [][]string
	WatchedKeys map[string]struct{}
}

func NewClientTxState() *ClientTxState {
	return &ClientTxState{
		InTx:        false,
		Queue:       make([][]string, 0),
		WatchedKeys: make(map[string]struct{}),
	}
}

// global registry for watched keys across all clients (to check modification upon EXEC)
type WatchManager struct {
	mu sync.Mutex
	// maps key -> slice of channels or state pointers if needed, or version tracking
	keyVersions map[string]uint64
}

/*var watchManager = &WatchManager{
	keyVersions: make(map[string]uint64),
}*/

func (wm *WatchManager) TouchKey(key string) {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	wm.keyVersions[key]++
}

func (wm *WatchManager) GetKeyVersion(key string) uint64 {
	wm.mu.Lock()
	defer wm.mu.Unlock()

	return wm.keyVersions[key]
}

// HandleWatch processes WATCH commands for a connection
func (tx *ClientTxState) HandleWatch(args []string) []byte {
	if tx.InTx {
		return []byte("-ERR WATCH inside MULTI not allowed\r\n")
	}

	if len(args) < 2 {
		return []byte("-ERR wrong number of arguments for 'watch' command\r\n")
	}

	for _, key := range args[1:] {
		tx.WatchedKeys[key] = struct{}{}
	}

	return []byte("+OK\r\n")
}

// HandleMulti initializes transaction mode
func (tx *ClientTxState) HandleMulti() []byte {
	if tx.InTx {
		return []byte("-ERR MULTI calls con not be nested\r\n")
	}

	tx.InTx = true
	tx.Queue = nil
	return []byte("+OK\r\n")
}

// reset clears transaction and watch state
func (tx *ClientTxState) Reset() {
	tx.InTx = false
	tx.Queue = nil
	tx.WatchedKeys = make(map[string]struct{})
}
