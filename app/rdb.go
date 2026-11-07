package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

type RDBReader struct {
	reader io.Reader
}

func readLength(r io.Reader) (length int, isSpecial bool, format byte, err error) {
	var firstByte [1]byte
	if _, err := io.ReadFull(r, firstByte[:]); err != nil {
		return 0, false, 0, err
	}

	b := firstByte[0]
	mode := b >> 6

	switch mode {
	case 0b00:
		return int(b & 0x3F), false, 0, nil
	case 0b01:
		var secondByte [1]byte
		if _, err := io.ReadFull(r, secondByte[:]); err != nil {
			return 0, false, 0, err
		}
		lenVal := (int(b&0x3F) << 8) | int(secondByte[0])
		return lenVal, false, 0, nil
	case 0b10:
		var buf [4]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, false, 0, err
		}
		return int(binary.BigEndian.Uint32(buf[:])), false, 0, nil
	case 0b11:
		return 0, true, b, nil
	}

	return 0, false, 0, fmt.Errorf("invalid length encoding")
}

func readString(r io.Reader) (string, error) {
	length, isSpecial, format, err := readLength(r)
	if err != nil {
		return "", err
	}

	if isSpecial {
		switch format {
		case 0xC0: // 8-bit integer
			var b [1]byte
			if _, err := io.ReadFull(r, b[:]); err != nil {
				return "", err
			}
			return strconv.Itoa(int(int8(b[0]))), nil
		case 0xC1: // 16-bit integer (little endian)
			var b [2]byte
			if _, err := io.ReadFull(r, b[:]); err != nil {
				return "", err
			}
			val := int16(binary.LittleEndian.Uint16(b[:]))
			return strconv.Itoa(int(val)), nil
		case 0xC2: // 32-bit integer (little endian)
			var b [4]byte
			if _, err := io.ReadFull(r, b[:]); err != nil {
				return "", err
			}
			val := int32(binary.LittleEndian.Uint32(b[:]))
			return strconv.Itoa(int(val)), nil
		default:
			return "", fmt.Errorf("unsupported special string encoding: 0x%X", format)
		}
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	return string(buf), nil
}

// LoadRDB parses the RDB file specified by dir and dbfilename into the Store
func LoadRDB(dir, filename string, store *Store) error {
	if dir == "" || filename == "" {
		return nil
	}

	fullPath := filepath.Join(dir, filename)
	file, err := os.Open(fullPath)
	if err != nil {
		// Non-existent file is treated as an empty DB
		return nil
	}
	defer file.Close()

	// 1. Read header (9 bytes: REDIS0011)
	header := make([]byte, 9)
	if _, err := io.ReadFull(file, header); err != nil {
		return fmt.Errorf("failed to read RDB header: %w", err)
	}

	var expiryTimestamp *time.Time

	for {
		var opCode [1]byte
		if _, err := io.ReadFull(file, opCode[:]); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		switch opCode[0] {
		case 0xFA: // Metadata subsection
			_, err := readString(file) // key
			if err != nil {
				return err
			}
			_, err = readString(file) // value
			if err != nil {
				return err
			}

		case 0xFE: // Database selector
			_, _, _, err := readLength(file)
			if err != nil {
				return err
			}

		case 0xFB: // Hash table resize info
			_, _, _, err := readLength(file) // total keys
			if err != nil {
				return err
			}
			_, _, _, err = readLength(file) // expires count
			if err != nil {
				return err
			}

		case 0xFC: // Expire time in milliseconds (8-byte unsigned integer, little-endian)
			var msBuf [8]byte
			if _, err := io.ReadFull(file, msBuf[:]); err != nil {
				return err
			}
			ms := binary.LittleEndian.Uint64(msBuf[:])
			t := time.UnixMilli(int64(ms))
			expiryTimestamp = &t

		case 0xFD: // Expire time in seconds (4-byte unsigned integer, little-endian)
			var secBuf [4]byte
			if _, err := io.ReadFull(file, secBuf[:]); err != nil {
				return err
			}
			sec := binary.LittleEndian.Uint32(secBuf[:])
			t := time.Unix(int64(sec), 0)
			expiryTimestamp = &t

		case 0xFF: // End of file
			return nil

		default:
			// Value type flag (0x00 for string)
			valueType := opCode[0]
			key, err := readString(file)
			if err != nil {
				return err
			}

			if valueType == 0x00 { // String value
				val, err := readString(file)
				if err != nil {
					return err
				}

				if expiryTimestamp != nil {
					if expiryTimestamp.After(time.Now()) {
						duration := time.Until(*expiryTimestamp)
						store.Set(key, val, duration)
					}
					expiryTimestamp = nil
				} else {
					store.Set(key, val, 0)
				}
			}
		}
	}

	return nil
}
