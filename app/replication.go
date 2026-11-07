package main

import (
	"bufio"
	"encoding/base64"
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

// Base64-encoded empty RDB file (version 11)
const emptyRDBBase64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

type ServerConfig struct {
	Port             int
	Role             string // "master" or "slave"
	MasterHost       string
	MasterPort       int
	MasterReplID     string
	MasterReplOffset int
	Dir              string
	DBFileName       string
	AppendOnly       string
	AppendDirName    string
	AppendFileName   string
	AppendFsync      string
}

type ReplicaConnection struct {
	conn       net.Conn
	mu         sync.Mutex
	lastAckOff int
	notify     chan struct{}
}

type ReplicaRegistry struct {
	mu           sync.Mutex
	replicas     []*ReplicaConnection
	masterOffset int
}

var replicaRegistry = &ReplicaRegistry{
	replicas:     make([]*ReplicaConnection, 0),
	masterOffset: 0,
}

func (r *ReplicaRegistry) RegisterReplica(conn net.Conn, reader *bufio.Reader) {
	rep := &ReplicaConnection{
		conn:       conn,
		lastAckOff: 0,
		notify:     make(chan struct{}, 1),
	}

	r.mu.Lock()
	r.replicas = append(r.replicas, rep)
	r.mu.Unlock()

	// Dedicated reader loop for this replica connection on the master
	go func() {
		defer conn.Close()
		for {
			args, _, err := readRESPArrayWithBytes(reader)
			if err != nil {
				return
			}
			if len(args) >= 3 && strings.ToUpper(args[0]) == "REPLCONF" && strings.ToUpper(args[1]) == "ACK" {
				if off, err := strconv.Atoi(args[2]); err == nil {
					rep.mu.Lock()
					rep.lastAckOff = off
					rep.mu.Unlock()

					select {
					case rep.notify <- struct{}{}:
					default:
					}
				}
			}
		}
	}()
}

func (r *ReplicaRegistry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.replicas)
}

func (r *ReplicaRegistry) GetMasterOffset() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.masterOffset
}

func (r *ReplicaRegistry) Propagate(rawCommand []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.masterOffset += len(rawCommand)

	active := make([]*ReplicaConnection, 0, len(r.replicas))
	for _, rep := range r.replicas {
		_, err := rep.conn.Write(rawCommand)
		if err == nil {
			active = append(active, rep)
		} else {
			_ = rep.conn.Close()
		}
	}
	r.replicas = active
}

func (r *ReplicaRegistry) WaitForReplicas(numReplicas int, timeoutMs int) int {
	r.mu.Lock()
	total := len(r.replicas)
	target := r.masterOffset
	reps := append([]*ReplicaConnection(nil), r.replicas...)
	r.mu.Unlock()

	if total == 0 {
		return 0
	}

	// If no writes have been propagated, all replicas are considered in sync
	if target == 0 {
		return total
	}

	// Check how many replicas are ALREADY at target offset
	currentAcks := 0
	for _, rep := range reps {
		rep.mu.Lock()
		if rep.lastAckOff >= target {
			currentAcks++
		}
		rep.mu.Unlock()
	}

	if currentAcks >= numReplicas {
		return currentAcks
	}

	// Request ACKs from replicas
	getAckCmd := "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"
	for _, rep := range reps {
		_, _ = rep.conn.Write([]byte(getAckCmd))
	}

	deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)

	for {
		timeLeft := time.Until(deadline)
		if timeLeft <= 0 {
			break
		}

		// Re-count ACKs
		count := 0
		for _, rep := range reps {
			rep.mu.Lock()
			if rep.lastAckOff >= target {
				count++
			}
			rep.mu.Unlock()
		}

		if count >= numReplicas {
			return count
		}

		time.Sleep(10 * time.Millisecond)
	}

	finalCount := 0
	for _, rep := range reps {
		rep.mu.Lock()
		if rep.lastAckOff >= target {
			finalCount++
		}
		rep.mu.Unlock()
	}
	return finalCount
}

func ParseServerConfig() ServerConfig {
	getArgVal := func(flagName string, defaultVal string) string {
		for i := 1; i < len(os.Args); i++ {
			arg := os.Args[i]
			// Matches --flag=val or -flag=val
			prefix1 := "--" + flagName + "="
			prefix2 := "-" + flagName + "="
			if strings.HasPrefix(arg, prefix1) {
				return arg[len(prefix1):]
			}
			if strings.HasPrefix(arg, prefix2) {
				return arg[len(prefix2):]
			}

			// Matches --flag val or -flag val
			if (arg == "--"+flagName || arg == "-"+flagName) && i+1 < len(os.Args) {
				return os.Args[i+1]
			}

			// Matches --flagval (if attached directly)
			prefix3 := "--" + flagName
			if strings.HasPrefix(arg, prefix3) && len(arg) > len(prefix3) && arg[len(prefix3)] != '-' {
				return arg[len(prefix3):]
			}
		}
		return defaultVal
	}

	portStr := getArgVal("port", "6379")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		port = 6379
	}

	cwd, _ := os.Getwd()
	dirVal := getArgVal("dir", cwd)
	if dirVal == "" {
		dirVal = cwd
	}

	config := ServerConfig{
		Port:             port,
		Role:             "master",
		MasterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		MasterReplOffset: 0,
		Dir:              dirVal,
		DBFileName:       getArgVal("dbfilename", "dump.rdb"),
		AppendOnly:       getArgVal("appendonly", "no"),
		AppendDirName:    getArgVal("appenddirname", "appendonlydir"),
		AppendFileName:   getArgVal("appendfilename", "appendonly.aof"),
		AppendFsync:      getArgVal("appendfsync", "everysec"),
	}

	replicaArg := getArgVal("replicaof", "")
	if replicaArg != "" {
		parts := strings.Fields(replicaArg)
		if len(parts) >= 2 {
			config.Role = "slave"
			config.MasterHost = parts[0]
			mPort, err := strconv.Atoi(parts[1])
			if err == nil {
				config.MasterPort = mPort
			}
		}
	}

	return config
}

func (c ServerConfig) Address() string {
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

func (c ServerConfig) GetReplicationInfo() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("role:%s\r\n", c.Role))

	if c.Role == "master" {
		sb.WriteString(fmt.Sprintf("master_replid:%s\r\n", c.MasterReplID))
		sb.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", replicaRegistry.GetMasterOffset()))
	}

	return sb.String()
}

func EncodeRESPArray(args []string) []byte {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	return []byte(sb.String())
}

func HandleMasterPSYNC(conn net.Conn, reader *bufio.Reader, config ServerConfig) error {
	fullResyncResp := fmt.Sprintf("+FULLRESYNC %s 0\r\n", config.MasterReplID)
	if _, err := conn.Write([]byte(fullResyncResp)); err != nil {
		return err
	}

	rdbBytes, err := base64.StdEncoding.DecodeString(emptyRDBBase64)
	if err != nil {
		return fmt.Errorf("failed to decode empty rdb: %w", err)
	}

	header := fmt.Sprintf("$%d\r\n", len(rdbBytes))
	payload := append([]byte(header), rdbBytes...)

	if _, err := conn.Write(payload); err != nil {
		return err
	}

	// Register with the existing reader so no bytes are lost or contested
	replicaRegistry.RegisterReplica(conn, reader)
	return nil
}

func readRESPArrayWithBytes(reader *bufio.Reader) ([]string, int, error) {
	bytesConsumed := 0

	readRESPLine := func() (string, int, error) {
		lineBytes := make([]byte, 0, 64)
		for {
			b, err := reader.ReadByte()
			if err != nil {
				return "", len(lineBytes), err
			}
			lineBytes = append(lineBytes, b)
			if len(lineBytes) >= 2 && lineBytes[len(lineBytes)-2] == '\r' && lineBytes[len(lineBytes)-1] == '\n' {
				break
			}
		}
		rawLen := len(lineBytes)
		content := string(lineBytes[:rawLen-2])
		return content, rawLen, nil
	}

	header, n, err := readRESPLine()
	if err != nil {
		return nil, n, err
	}
	bytesConsumed += n

	if len(header) == 0 || header[0] != '*' {
		return nil, bytesConsumed, fmt.Errorf("expected array prefix '*', got: %q", header)
	}

	numElements, err := strconv.Atoi(header[1:])
	if err != nil {
		return nil, bytesConsumed, fmt.Errorf("invalid array length %q: %w", header[1:], err)
	}

	args := make([]string, 0, numElements)
	for i := 0; i < numElements; i++ {
		strHeader, n, err := readRESPLine()
		if err != nil {
			return nil, bytesConsumed, err
		}
		bytesConsumed += n

		if len(strHeader) == 0 || strHeader[0] != '$' {
			return nil, bytesConsumed, fmt.Errorf("expected bulk string prefix '$', got: %q", strHeader)
		}

		strLen, err := strconv.Atoi(strHeader[1:])
		if err != nil {
			return nil, bytesConsumed, fmt.Errorf("invalid bulk string length %q: %w", strHeader[1:], err)
		}

		buf := make([]byte, strLen)
		nRead, err := io.ReadFull(reader, buf)
		bytesConsumed += nRead
		if err != nil {
			return nil, bytesConsumed, err
		}

		crlf := make([]byte, 2)
		nRead, err = io.ReadFull(reader, crlf)
		bytesConsumed += nRead
		if err != nil {
			return nil, bytesConsumed, err
		}

		args = append(args, string(buf))
	}

	return args, bytesConsumed, nil
}

func StartReplicationHandshake(config ServerConfig, store *Store) {
	masterAddr := fmt.Sprintf("%s:%d", config.MasterHost, config.MasterPort)
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Printf("Failed to connect to master %s: %s\n", masterAddr, err.Error())
		return
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	sendAndReceive := func(payload string, desc string) (string, error) {
		if _, err := conn.Write([]byte(payload)); err != nil {
			return "", fmt.Errorf("failed to send %s: %w", desc, err)
		}
		resp, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("failed to read response for %s: %w", desc, err)
		}
		return resp, nil
	}

	if _, err := sendAndReceive("*1\r\n$4\r\nPING\r\n", "PING"); err != nil {
		fmt.Println(err.Error())
		return
	}

	portStr := strconv.Itoa(config.Port)
	replconfPortCmd := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(portStr),
		portStr,
	)
	if _, err := sendAndReceive(replconfPortCmd, "REPLCONF listening-port"); err != nil {
		fmt.Println(err.Error())
		return
	}

	replconfCapaCmd := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	if _, err := sendAndReceive(replconfCapaCmd, "REPLCONF capa psync2"); err != nil {
		fmt.Println(err.Error())
		return
	}

	psyncCmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	psyncResp, err := sendAndReceive(psyncCmd, "PSYNC ? -1")
	if err != nil {
		fmt.Println(err.Error())
		return
	}

	if strings.HasPrefix(psyncResp, "+FULLRESYNC") {
		rdbHeader, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Failed to read RDB header:", err.Error())
			return
		}

		trimmedHeader := strings.TrimRight(rdbHeader, "\r\n")
		if strings.HasPrefix(trimmedHeader, "$") {
			rdbLen, err := strconv.Atoi(trimmedHeader[1:])
			if err == nil && rdbLen > 0 {
				rdbBuf := make([]byte, rdbLen)
				if _, err := io.ReadFull(reader, rdbBuf); err != nil {
					fmt.Println("Failed to read RDB binary content:", err.Error())
					return
				}
			}
		}
	}

	processedOffset := 0

	for {
		args, cmdBytes, err := readRESPArrayWithBytes(reader)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			fmt.Println("Master connection read error:", err.Error())
			break
		}

		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])

		if cmd == "REPLCONF" && len(args) >= 2 && strings.ToUpper(args[1]) == "GETACK" {
			offsetStr := strconv.Itoa(processedOffset)
			ackPayload := fmt.Sprintf(
				"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%s\r\n",
				len(offsetStr),
				offsetStr,
			)

			if _, err := conn.Write([]byte(ackPayload)); err != nil {
				fmt.Println("Failed to send REPLCONF ACK:", err.Error())
				break
			}

			processedOffset += cmdBytes
			continue
		}

		executeCommand(args, store, config)
		processedOffset += cmdBytes
	}
}
