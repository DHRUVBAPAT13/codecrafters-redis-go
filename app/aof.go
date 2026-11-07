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

// InitAOF sets up directory, discovers the active incremental file from the manifest, and opens it
func InitAOF(config ServerConfig) error {

	if strings.ToLower(config.AppendOnly) != "yes" {
		return nil
	}

	aofDir := filepath.Join(config.Dir, config.AppendFileName)
	if err := os.MkdirAll(aofDir, 0755); err != nil {
		return fmt.Errorf("failed to create AOF directory: %w", err)
	}

	manifestPath := filepath.Join(aofDir, fmt.Sprintf("%s.manifest", config.AppendFileName))
	activeIncrFile := ""

	// chek if manifest already exists (e.g. provided by master)
	if data, err := os.ReadFile(manifestPath); err != nil {

		scanner := bufio.NewScanner(strings.NewReader(string(data)))
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			parts := strings.Fields(line)

			// format: file <filename> seq <seq> type <type>
			if len(parts) >= 6 && parts[0] == "file" && parts[4] == "type" {
				activeIncrFile = parts[1]
				break
			}
		}
	}

	// if no manifest or no incremental file entry found, create defaults
	if activeIncrFile == "" {
		activeIncrFile = fmt.Sprintf("%s.1.incr.aof", config.AppendFileName)
		manifestContent := fmt.Sprintf("file %s seq 1 type i\n", activeIncrFile)

		if err := os.WriteFile(manifestPath, []byte(manifestContent), 0644); err != nil {
			return fmt.Errorf("failed to write AOF mainfest: %w", err)
		}
	}

	activeFilePath := filepath.Join(aofDir, activeIncrFile)
	f, err := os.OpenFile(activeFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)

	if err != nil {
		return fmt.Errorf("failed to open AOF file %s: %w", activeFilePath, err)
	}

	aofManager.mu.Lock()
	aofManager.enabled = true
	aofManager.filePath = activeFilePath
	aofManager.appendFsync = strings.ToLower(config.AppendFsync)
	aofManager.file = f
	aofManager.mu.Unlock()

	return nil

}

func (a *AOFManager) AppendCommand(rawCommand []byte) error {

	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.enabled || a.file == nil {
		return nil
	}

	if _, err := a.file.Write(rawCommand); err != nil {
		return err
	}

	if a.appendFsync == "yes" {
		return a.file.Sync()
	}

	return nil
}
