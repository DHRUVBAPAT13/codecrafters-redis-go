package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AOFManager struct {
	mu          sync.Mutex
	enabled     bool
	filePath    string
	appendFsync string
	file        *os.File
}

var aofManager = &AOFManager{}

// replayAOFFile parses and executes all RESP commands in the incremental AOF file
func replayAOFFile(path string, store *Store) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		// Read array header: *<count>
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			continue
		}

		if !strings.HasPrefix(line, "*") {
			continue
		}

		numArgs, err := strconv.Atoi(line[1:])
		if err != nil {
			return err
		}

		args := make([]string, 0, numArgs)
		for i := 0; i < numArgs; i++ {
			// Read bulk string header: $<len>
			lenLine, err := reader.ReadString('\n')
			if err != nil {
				return err
			}
			lenLine = strings.TrimRight(lenLine, "\r\n")
			if !strings.HasPrefix(lenLine, "$") {
				return fmt.Errorf("expected $, got %q", lenLine)
			}

			strLen, err := strconv.Atoi(lenLine[1:])
			if err != nil {
				return err
			}

			// Read exact bytes of the bulk string
			buf := make([]byte, strLen)
			if _, err := io.ReadFull(reader, buf); err != nil {
				return err
			}
			args = append(args, string(buf))

			// Discard the trailing newline (\r\n or \n)
			discardByte, err := reader.ReadByte()
			if err != nil && !errors.Is(err, io.EOF) {
				return err
			}
			if discardByte == '\r' {
				_, _ = reader.ReadByte() // Consume the \n after \r
			}
		}

		if len(args) > 0 {
			executeCommandForReplay(args, store)
		}
	}

	return nil
}

// InitAOF sets up directory, discovers the active incremental file from the manifest, and opens it.
func InitAOF(config ServerConfig, store *Store) error {
	if strings.ToLower(config.AppendOnly) != "yes" {
		return nil
	}

	aofDir := filepath.Join(config.Dir, config.AppendDirName)
	if err := os.MkdirAll(aofDir, 0755); err != nil {
		return fmt.Errorf("failed to create AOF dir: %w", err)
	}

	manifestPath := filepath.Join(aofDir, fmt.Sprintf("%s.manifest", config.AppendFileName))
	activeIncrFile := ""

	// 1. Read existing manifest if present
	if data, err := os.ReadFile(manifestPath); err == nil {
		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			parts := strings.Fields(line)
			// Format: file <filename> seq <seq> type <type>
			if len(parts) >= 6 && parts[0] == "file" && parts[4] == "type" && parts[5] == "i" {
				activeIncrFile = parts[1]
				break
			}
		}
	}

	// 2. If no manifest or no incremental entry found, create defaults
	if activeIncrFile == "" {
		activeIncrFile = fmt.Sprintf("%s.1.incr.aof", config.AppendFileName)
		manifestContent := fmt.Sprintf("file %s seq 1 type i\n", activeIncrFile)
		_ = os.WriteFile(manifestPath, []byte(manifestContent), 0644)
	}

	activeFilePath := filepath.Join(aofDir, activeIncrFile)

	// 3. replay existing commands if file already exists
	if _, err := os.Stat(activeFilePath); err == nil {

		if err := replayAOFFile(activeFilePath, store); err != nil {
			fmt.Printf("AOF replay warning on %s: %s\n", activeFilePath, err.Error())
		}
	}

	// 4. Open with os.O_CREATE|os.O_WRONLY|os.O_APPEND for incoming client commands
	f, err := os.OpenFile(activeFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open/create AOF file %s: %w", activeFilePath, err)
	}

	aofManager.mu.Lock()
	aofManager.enabled = true
	aofManager.filePath = activeFilePath
	aofManager.appendFsync = strings.ToLower(config.AppendFsync)
	aofManager.file = f
	aofManager.mu.Unlock()

	return nil
}

// AppendCommand writes raw RESP command bytes and flushes if required
func (a *AOFManager) AppendCommand(rawCommand []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.enabled || a.file == nil {
		return nil
	}

	if _, err := a.file.Write(rawCommand); err != nil {
		return err
	}

	if a.appendFsync == "always" {
		return a.file.Sync()
	}

	return nil
}

// executeCommandForReplay handles commands like SET during recovery without side effects
func executeCommandForReplay(args []string, store *Store) {
	if len(args) == 0 {
		return
	}
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "SET":
		if len(args) >= 3 {
			key := args[1]
			val := args[2]

			var expiry time.Duration
			for i := 3; i < len(args); i++ {
				if strings.ToUpper(args[i]) == "PX" && i+1 < len(args) {
					if ms, err := strconv.ParseInt(args[i+1], 10, 64); err == nil {
						expiry = time.Duration(ms) * time.Millisecond
					}
					i++
				}
			}
			store.Set(key, val, expiry)
		}

	case "ZADD":
		if len(args) >= 4 {
			key := args[1]
			if score, err := strconv.ParseFloat(args[2], 64); err != nil {
				member := args[3]
				store.ZAdd(key, score, member)
			}
		}

	case "ZREM":
		if len(args) >= 3 {
			store.ZRem(args[1], args[2:]...)
		}

	case "SETBIT":
		if len(args) >= 4 {
			key := args[1]
			offset, err1 := strconv.Atoi(args[2])
			bit, err2 := strconv.Atoi(args[3])

			if err1 == nil && err2 == nil && (bit == 0 || bit == 1) && offset >= 0 {
				_, _ = store.SetBit(key, offset, bit)
			}
		}
	}
}
