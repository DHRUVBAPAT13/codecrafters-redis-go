package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Item struct {
	value     string
	expiresAt time.Time // zero time means no expiration
}

func (item Item) IsExpired() bool {
	if item.expiresAt.IsZero() {
		return false
	}
	return time.Now().After(item.expiresAt)
}

// In-memory key-value store with read-write mutex for concurrency safety
type Store struct {
	mu      sync.RWMutex
	data    map[string]Item
	lists   map[string][]string
	blocked map[string][]chan string
}

func NewStore() *Store {
	return &Store{
		data:    make(map[string]Item),
		lists:   make(map[string][]string),
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

	if item.IsExpired() {
		// passive deletion
		s.mu.Lock()
		delete(s.data, key)
		s.mu.Unlock()
		return "", false
	}

	return item.value, true
}

func (s *Store) RPush(key string, values ...string) int {

	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. append elements to the list
	s.lists[key] = append(s.lists[key], values...)
	newLen := len(s.lists[key])

	// 2. deliver elements to blocked clients in FIFO order
	for len(s.blocked[key]) > 0 && len(s.lists[key]) > 0 {
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
		ch <- val
	}
	return newLen
}

func (s *Store) LPush(key string, values ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. prepend elements in reverse order
	reversed := make([]string, len(values))
	for i, v := range values {
		reversed[len(values)-1-i] = v
	}
	s.lists[key] = append(reversed, s.lists[key]...)
	newLen := len(s.lists[key])

	// 2. deliver elements to waiting blocked clients in FIFO order
	for len(s.blocked[key]) > 0 && len(s.lists[key]) > 0 {
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
		ch <- val
	}
	return newLen
}

func (s *Store) LRange(key string, start, stop int) []string {

	s.mu.RLock()
	defer s.mu.RUnlock()

	list, ok := s.lists[key]
	if !ok || len(list) == 0 {
		return nil
	}

	n := len(list)

	// convert negative start index
	if start < 0 {
		start = n + start
		if start < 0 {
			start = 0
		}
	}

	// convert negative stop index
	if stop < 0 {
		stop = n + stop
		if stop < 0 {
			stop = 0
		}
	}

	// standard Redis boundary checks after normalization
	if start >= n || start > stop {
		return []string{}
	}

	// clamp stop at last element
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

// BLPop attempts to pop an item, blocking up to timeout duration.
// A timeout of 0 indicates indefinite wait.
// Returns (value, true) if an item was acquired, or ("", false) on timeout.
func (s *Store) BLPop(key string, timeout time.Duration) (string, bool) {
	s.mu.Lock()

	// 1. If an element already exists, pop immediately
	if list, ok := s.lists[key]; ok && len(list) > 0 {
		val := list[0]
		s.lists[key] = list[1:]
		if len(s.lists[key]) == 0 {
			delete(s.lists, key)
		}
		s.mu.Unlock()
		return val, true
	}

	// 2. register a channel in the waiting queue (FIFO)
	ch := make(chan string, 1)
	s.blocked[key] = append(s.blocked[key], ch)
	s.mu.Unlock()

	// 3. wait indefinitely if timeout is 0
	if timeout == 0 {
		val := <-ch
		return val, true
	}

	// 4. wait with timeout
	select {
	case val := <-ch:
		return val, true

	case <-time.After(timeout):
		// clean up channel from s.blocked on timeout
		s.mu.Unlock()
		waiters := s.blocked[key]
		for i, waiter := range waiters {
			if waiter == ch {
				s.blocked[key] = append(waiters[:i], waiters[i+1:1]...)
				break
			}
		}
		if len(s.blocked[key]) == 0 {
			delete(s.blocked, key)
		}
		s.mu.Unlock()

		// check if an item was just sent as the timeout triggered
		select {
		case val := <-ch:
			return val, true

		default:
			return "", false
		}
	}
}

// readRESPArray parses an incoming Redis command formatted as a RESP Array (*<count>\r\n...)
func readRESPArray(reader *bufio.Reader) ([]string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	line = strings.TrimSuffix(line, "\r\n")
	line = strings.TrimSuffix(line, "\n")

	if len(line) == 0 {
		return nil, nil
	}

	if line[0] != '*' {
		parts := strings.Fields(line)
		return parts, nil
	}

	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}

	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		header = strings.TrimSuffix(header, "\r\n")
		header = strings.TrimSuffix(header, "\n")

		if len(header) == 0 || header[0] != '$' {
			return nil, fmt.Errorf("expected bulk string prefix '$', got: %s", header)
		}

		length, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}

		dataBuf := make([]byte, length+2)
		_, err = io.ReadFull(reader, dataBuf)
		if err != nil {
			return nil, err
		}

		args = append(args, string(dataBuf[:length]))
	}

	return args, nil
}

func handleClient(conn net.Conn, store *Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	for {
		args, err := readRESPArray(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Println("Connection read error:", err.Error())
			break
		}

		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		switch cmd {
		case "PING":
			_, _ = conn.Write([]byte("+PONG\r\n"))

		case "ECHO":
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				continue
			}
			arg := args[1]
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
			_, _ = conn.Write([]byte(resp))

		case "SET":
			if len(args) < 3 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'set' command\r\n"))
				continue
			}
			key := args[1]
			val := args[2]
			var ttl time.Duration

			// Parse optional arguments (case-insensitive) like PX <ms> or EX <s>
			for i := 3; i < len(args); i++ {

				opt := strings.ToUpper(args[i])

				if opt == "PX" && i+1 < len(args) {
					ms, err := strconv.ParseInt(args[i+1], 10, 64)
					if err == nil {
						ttl = time.Duration(ms) * time.Millisecond
					}
					i++
				} else if opt == "EX" && i+1 < len(args) {
					sec, err := strconv.ParseInt(args[i+1], 10, 64)
					if err == nil {
						ttl = time.Duration(sec) * time.Second
					}
					i++
				}
			}

			store.Set(key, val, ttl)
			_, _ = conn.Write([]byte("+OK\r\n"))

		case "GET":
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'get' command\r\n"))
				continue
			}
			key := args[1]
			val, ok := store.Get(key)
			if !ok {
				// Null Bulk String for missing keys
				_, _ = conn.Write([]byte("$-1\r\n"))
			} else {
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
				_, _ = conn.Write([]byte(resp))
			}

		case "RPUSH":

			if len(args) < 3 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'rpush' command\r\n"))
				continue
			}
			key := args[1]
			// args[2:] naturally supports one or multiple elements
			newLen := store.RPush(key, args[2:]...)
			resp := fmt.Sprintf(":%d\r\n", newLen)
			_, _ = conn.Write([]byte(resp))

		case "LPUSH":
			if len(args) < 3 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n"))
				continue
			}
			key := args[1]
			// args[2:] naturally supports one or multiple elements
			newLen := store.LPush(key, args[2:]...)
			resp := fmt.Sprintf(":%d\r\n", newLen)
			_, _ = conn.Write([]byte(resp))

		case "LRANGE":

			if len(args) < 4 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'lrange' command\r\n"))
				continue
			}

			key := args[1]
			start, err1 := strconv.Atoi(args[2])
			stop, err2 := strconv.Atoi(args[3])

			if err1 != nil || err2 != nil {
				_, _ = conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}

			items := store.LRange(key, start, stop)

			// format as RESP Array *<count>\r\n$<len>\r\n<item>\r\n...

			var resp strings.Builder
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(items)))

			for _, item := range items {
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
			}
			_, _ = conn.Write([]byte(resp.String()))

		case "LLEN":
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'llen' command\r\n"))
				continue
			}
			key := args[1]
			length := store.LLen(key)
			resp := fmt.Sprintf(":%d\r\n", length)
			_, _ = conn.Write([]byte(resp))

		case "LPOP":
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'lpop' command\r\n"))
				continue
			}
			key := args[1]
			hasCount := len(args) >= 3

			count := 1
			if hasCount {
				parsedCount, err := strconv.Atoi(args[2])
				if err != nil || parsedCount <= 0 {
					_, _ = conn.Write([]byte("-ERR value is not a positive integer\r\n"))
					continue
				}
				count = parsedCount
			}

			popped, ok := store.LPop(key, count)
			if !ok {
				// null bulk string for empty list
				_, _ = conn.Write([]byte("$-1\r\n"))
			}

			if !hasCount {
				// single element format: bulk string
				val := popped[0]
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
				_, _ = conn.Write([]byte(resp))
			} else {
				// multi-elements format: RESP Array
				var resp strings.Builder
				resp.WriteString(fmt.Sprintf("*%d\r\n", len(popped)))
				for _, item := range popped {
					resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
				}
				_, _ = conn.Write([]byte(resp.String()))
			}

		case "BLPOP":

			if len(args) < 3 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
				continue
			}

			key := args[1]
			timeoutSec, err := strconv.ParseFloat(args[2], 64)
			if err != nil || timeoutSec < 0 {
				_, _ = conn.Write([]byte("-ERR timeout is not a float or out of range"))
				continue
			}

			var timeout time.Duration
			if timeoutSec > 0 {
				timeout = time.Duration(timeoutSec * float64(time.Second))
			}

			val, ok := store.BLPop(key, timeout)
			if !ok {
				// null array on timeout: *-1\r\n
				_, _ = conn.Write([]byte("*-1\r\n"))
			} else {
				// RESP array: [key, val]
				resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
				_, _ = conn.Write([]byte(resp))
			}

		default:
			errMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd)
			_, _ = conn.Write([]byte(errMsg))
		}
	}
}

func main() {
	store := NewStore()

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379:", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}

		go handleClient(conn, store)
	}
}
