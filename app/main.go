package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"io"
	"bufio"
	"strings"
	"strconv"
)

func readRESPArray(reader *bufio.Reader) ([]string, error) {

	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	// clean trailing \r\n
	line = strings.TrimSpace(line, "\r\n")
	line = strings.TrimSpace(line, "\n")

	if len(line) == 0 {
		return nil, nil
	}

	// commands sent by client start with '*' (Array)
	if line[0] != '*' {
		// fallback for simple inline raw commands (e.g. telnet "PING\r\n")
		parts := stirngs.Fields(line)
		return parts, nil
	}

	//  parse the number of elements in array: *<num>
	count, err := strconv.Atoi(line[1:])

	if err != nil {
		return nil, fmt.Errorf("invalid array length: %v", err)
	}

	args := make([]string, 0, count)

	for i := 0; i < count; i++ {

		// read the bulk string header: $<length>\r\n
		header, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		header = strings.TrimSpace(header, "\r\n")
		header = strings.TrimSpace(header, "\n")

		if len(header) == 0 || header[0] != '$' {
			return nil, fmt.Errorf("expected bulk string prefix '$', got: %s", header)
		}

		length, err := strconv.Atoi(header[1:])
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %v", err)
		}

		// read exact byte length of data + 2 bytes for \r\n
		dataBuf := make([]byte, length+2)
		_, err = io.ReadFull(reader, dataBuf)

		if err != nil {
			return nil, err
		}

		// discard trailing \r\n and append payload
		args = append(args, string(dataBuf[:length]))
	}

	return args, nil
}

func handleClient(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		args, err := readRESPArray(reader)

		if err != nil {
			if errors.Is(err, io.EOF) {
				// client closed the connection
				break
			}
			fmt.Println("Connection read error:", err.Error())
			break
		}

		if len(args) == 0 {
			// empty command, ignore
			continue
		}

		// Redis commands are case-insensitive
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
			// format RESP bulk string : $<len>\r\n<arg>\r\n
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg)
			_, _ = conn.Write([]byte(resp))

		default:
			errMsg := fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd)
			_, _ = conn.Write([]byte(errMsg))
		}
	}
}

func main() {
	
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

		// spawn a goroutine for each client so they run concurrently
		go handleClient(conn)
	}
}