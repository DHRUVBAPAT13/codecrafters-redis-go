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
)

// In-memory key-value store with read-write mutex for concurrency safety
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.data[key]
	return val, ok
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
			store.Set(key, val)
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