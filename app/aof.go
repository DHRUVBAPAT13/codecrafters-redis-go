package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type AOFManager struct {
	mu          sync.Mutex
	enabled     bool
	filePath    string
	appendFsync string
	file        *os.File
}

var aofManager = &AOFManager{}

// InitAOF sets up directory, discovers the active incremental file from the manifest, and opens it.
func InitAOF(config ServerConfig) error {
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

	// 3. Open with os.O_CREATE to guarantee file exists on disk
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
