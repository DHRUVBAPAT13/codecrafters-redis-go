package main

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

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
