package main

import (
	"bufio"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Empty RDB file (version 11) represented in base64
const emptyRDBBase64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

type ServerConfig struct {
	Port             int
	Role             string // "master" or "slave"
	MasterHost       string
	MasterPort       int
	MasterReplID     string
	MasterReplOffset int
}

// ReplicaRegistry holds active replica connections for future command propagation
type ReplicaRegistry struct {
	mu       sync.Mutex
	replicas []net.Conn
}

var replicaRegistry = &ReplicaRegistry{
	replicas: make([]net.Conn, 0),
}

func (r *ReplicaRegistry) Register(conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.replicas = append(r.replicas, conn)
}

func ParseServerConfig() ServerConfig {
	port := flag.Int("port", 6379, "Port to listen on")
	replicaOf := flag.String("replicaof", "", "Master host and port to replicate")
	flag.Parse()

	config := ServerConfig{
		Port:             *port,
		Role:             "master",
		MasterReplID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
		MasterReplOffset: 0,
	}

	var replicaArg string
	if *replicaOf != "" {
		replicaArg = *replicaOf
	} else {
		for i, arg := range os.Args {
			if (arg == "--replicaof" || arg == "-replicaof") && i+1 < len(os.Args) {
				if i+2 < len(os.Args) && !strings.HasPrefix(os.Args[i+1], "-") && !strings.HasPrefix(os.Args[i+2], "-") {
					replicaArg = os.Args[i+1] + " " + os.Args[i+2]
				} else {
					replicaArg = os.Args[i+1]
				}
				break
			}
		}
	}

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
		sb.WriteString(fmt.Sprintf("master_repl_offset:%d\r\n", c.MasterReplOffset))
	}

	return sb.String()
}

// readRESPArrayWithBytes reads a RESP Array from reader and counts the total raw bytes consumed
func readRESPArrayWithBytes(reader *bufio.Reader) ([]string, int, error) {
	bytesConsumed := 0

	// Helper to read until \r\n and accurately track wire bytes
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

		// Read the trailing \r\n after the bulk string content
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

// HandleMasterPSYNC responds to PSYNC with +FULLRESYNC and transmits the empty RDB file
func HandleMasterPSYNC(conn net.Conn, config ServerConfig) error {
	fullResyncResp := fmt.Sprintf("+FULLRESYNC %s 0\r\n", config.MasterReplID)
	if _, err := conn.Write([]byte(fullResyncResp)); err != nil {
		return err
	}

	rdbBytes, err := base64.StdEncoding.DecodeString(emptyRDBBase64)
	if err != nil {
		return fmt.Errorf("failed to decode empty rdb: %w", err)
	}

	// Format: $<length>\r\n<contents> (no trailing \r\n)
	header := fmt.Sprintf("$%d\r\n", len(rdbBytes))
	payload := append([]byte(header), rdbBytes...)

	if _, err := conn.Write(payload); err != nil {
		return err
	}

	replicaRegistry.Register(conn)
	return nil
}

// StartReplicationHandshake connects to the master and runs replica handshake
func StartReplicationHandshake(config ServerConfig, store *Store) {

	masterAddr := fmt.Sprintf("%s:%d", config.MasterHost, config.MasterPort)
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		fmt.Printf("Failed to connect to master %s: %s\n", masterAddr, err.Error())
		return
	}

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

	// 1. PING
	if _, err := sendAndReceive("*1\r\n$4\r\nPING\r\n", "PING"); err != nil {
		fmt.Println(err.Error())
		conn.Close()
		return
	}

	// 2. REPLCONF listening-port <PORT>
	portStr := strconv.Itoa(config.Port)
	replconfPortCmd := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(portStr),
		portStr,
	)
	if _, err := sendAndReceive(replconfPortCmd, "REPLCONF listening-port"); err != nil {
		fmt.Println(err.Error())
		conn.Close()
		return
	}

	// 3. REPLCONF capa psync2
	replconfCapaCmd := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
	if _, err := sendAndReceive(replconfCapaCmd, "REPLCONF capa psync2"); err != nil {
		fmt.Println(err.Error())
		conn.Close()
		return
	}

	// 4. PSYNC ? -1
	psyncCmd := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	if _, err := conn.Write([]byte(psyncCmd)); err != nil {
		fmt.Println("Failed to send PSYNC:", err.Error())
		return
	}

	// Read "+FULLRESYNC <id> <offset>\r\n"
	psyncResp, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Failed to read PSYNC response:", err.Error())
		return
	}

	if strings.HasPrefix(psyncResp, "+FULLRESYNC") {
		// Read RDB header line: "$<len>\r\n"
		rdbHeader, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Failed to read RDB header:", err.Error())
			return
		}

		rdbHeader = strings.TrimSpace(rdbHeader)
		if strings.HasPrefix(rdbHeader, "$") {
			rdbLen, err := strconv.Atoi(rdbHeader[1:])
			if err == nil && rdbLen > 0 {
				rdbBuf := make([]byte, rdbLen)
				// Read EXACTLY rdbLen bytes without expecting trailing \r\n
				if _, err := io.ReadFull(reader, rdbBuf); err != nil {
					fmt.Println("Failed to read RDB binary content:", err.Error())
					return
				}
			}
		}
	}

	// 5. Continuous command processing loop from master
	processedOffset := 0

	for {
		// Use readRESPArrayWithBytes here:
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

			// Add the bytes of the GETACK command itself after sending the ACK
			processedOffset += cmdBytes
			continue
		}

		// Silently execute commands (SET, PING, etc.)
		executeCommand(args, store, config)

		// Increment offset by the bytes consumed for this command
		processedOffset += cmdBytes
	}
}

// EncodeRESPArray formats a slice of arguments into a RESP array byte slice
func EncodeRESPArray(args []string) []byte {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	return []byte(sb.String())
}

// Propagate broadcasts the raw RESP command to all registered replicas
func (r *ReplicaRegistry) Propagate(rawCommand []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	activeReplicas := make([]net.Conn, 0, len(r.replicas))
	for _, conn := range r.replicas {
		_, err := conn.Write(rawCommand)
		if err == nil {
			activeReplicas = append(activeReplicas, conn)
		} else {
			_ = conn.Close()
		}
	}
	r.replicas = activeReplicas
}

func (r *ReplicaRegistry) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.replicas)
}
