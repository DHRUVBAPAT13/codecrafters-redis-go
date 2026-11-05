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
	"time"
)

func handleClient(conn net.Conn, store *Store) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	inTx := false

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
			newLen := store.RPush(key, args[2:]...)
			resp := fmt.Sprintf(":%d\r\n", newLen)
			_, _ = conn.Write([]byte(resp))

		case "LPUSH":
			if len(args) < 3 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'lpush' command\r\n"))
				continue
			}
			key := args[1]
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
			resp := fmt.Sprintf(":%d\r\n", store.LLen(key))
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
				if err != nil || parsedCount < 0 {
					_, _ = conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
					continue
				}
				count = parsedCount
			}

			popped, ok := store.LPop(key, count)
			if !ok {
				_, _ = conn.Write([]byte("$-1\r\n"))
				continue
			}

			if !hasCount {
				val := popped[0]
				resp := fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)
				_, _ = conn.Write([]byte(resp))
			} else {
				var resp strings.Builder
				resp.WriteString(fmt.Sprintf("*%d\r\n", len(popped)))
				for _, val := range popped {
					resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))
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
				_, _ = conn.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
				continue
			}

			var timeout time.Duration
			if timeoutSec > 0 {
				timeout = time.Duration(timeoutSec * float64(time.Second))
			}

			val, ok := store.BLPop(key, timeout)
			if !ok {
				_, _ = conn.Write([]byte("*-1\r\n"))
			} else {
				resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
				_, _ = conn.Write([]byte(resp))
			}

		case "TYPE":
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'type' command\r\n"))
			}

			key := args[1]
			t := store.Type(key)
			resp := fmt.Sprintf("+%s\r\n", t)
			_, _ = conn.Write([]byte(resp))

		case "XADD":

			// Expected format: XADD key id field1 value1 [field2 value2 ...]
			// Minimum args: 4 (e.g. XADD stream 0-1 foo bar)
			if len(args) < 4 || (len(args)-3)%2 != 0 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'xadd' command\r\n"))
				continue
			}

			key := args[1]
			id := args[2]

			fields := make([]StreamField, 0, (len(args)-3)/2)
			for i := 3; i < len(args); i += 2 {
				fields = append(fields, StreamField{
					Key:   args[i],
					Value: args[i+1],
				})
			}

			entryID, err := store.XAdd(key, id, fields)

			if err != nil {
				resp := fmt.Sprintf("-%s\r\n", err.Error())
				_, _ = conn.Write([]byte(resp))
				continue
			}

			// Response is the entry ID encoded as a bulk string
			resp := fmt.Sprintf("$%d\r\n%s\r\n", len(entryID), entryID)
			_, _ = conn.Write([]byte(resp))

		case "XRANGE":
			if len(args) < 4 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'xrange' command\r\n"))
				continue
			}

			key := args[1]
			startBound := args[2]
			endBound := args[3]

			entries, err := store.XRange(key, startBound, endBound)
			if err != nil {
				resp := fmt.Sprintf("-%s\r\n", err.Error())
				_, _ = conn.Write([]byte(resp))
				continue
			}

			var resp strings.Builder
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))

			for _, entry := range entries {

				// each entry is a 2-element array: [ID, [k1, v1, k2, v2, ....]]
				resp.WriteString("*2\r\n")
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID))

				// field array length is len(fields)*2
				resp.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))

				for _, f := range entry.Fields {
					resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(f.Key), f.Key))
					resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(f.Value), f.Value))
				}
			}

			_, _ = conn.Write([]byte(resp.String()))

		case "XREAD":
			var blockTimeout time.Duration
			isBlock := false

			// Look for BLOCK option
			streamsIdx := -1
			for i := 1; i < len(args); i++ {
				argUpper := strings.ToUpper(args[i])
				if argUpper == "BLOCK" && i+1 < len(args) {
					ms, err := strconv.ParseInt(args[i+1], 10, 64)
					if err == nil {
						isBlock = true
						blockTimeout = time.Duration(ms) * time.Millisecond
					}
					i++
				} else if argUpper == "STREAMS" {
					streamsIdx = i
					break
				}
			}

			if streamsIdx == -1 || streamsIdx == len(args)-1 {
				_, _ = conn.Write([]byte("-ERR syntax error\r\n"))
				continue
			}

			streamArgs := args[streamsIdx+1:]
			if len(streamArgs)%2 != 0 {
				_, _ = conn.Write([]byte("-ERR syntax error, unmatched keys and IDs\r\n"))
				continue
			}

			numStreams := len(streamArgs) / 2
			keys := streamArgs[:numStreams]
			ids := streamArgs[numStreams:]

			// Resolve "$" to the current top entry ID for each stream
			for i := 0; i < numStreams; i++ {
				if ids[i] == "$" {
					ids[i] = store.GetLastID(keys[i])
				}
			}

			type streamResult struct {
				key     string
				entries []StreamEntry
			}

			fetchEntries := func() ([]streamResult, error) {
				var res []streamResult
				for i := 0; i < numStreams; i++ {
					entries, err := store.XRead(keys[i], ids[i])
					if err != nil {
						return nil, err
					}
					if len(entries) > 0 {
						res = append(res, streamResult{
							key:     keys[i],
							entries: entries,
						})
					}
				}
				return res, nil
			}

			results, err := fetchEntries()
			if err != nil {
				resp := fmt.Sprintf("-%s\r\n", err.Error())
				_, _ = conn.Write([]byte(resp))
				continue
			}

			// If empty and BLOCK was requested, wait for notifications
			if len(results) == 0 && isBlock {
				waitChans := make([]chan struct{}, len(keys))
				for i, k := range keys {
					waitChans[i] = registerStreamWaiter(k)
				}

				done := make(chan struct{})
				defer close(done) // Closes done when the wait finishes, killing all listener goroutines

				// Aggregator channel that signals when any key gets an item
				merged := make(chan struct{}, 1)
				for _, ch := range waitChans {
					go func(c chan struct{}) {
						select {
						case <-c:
							select {
							case merged <- struct{}{}:
							default:
							}
						case <-done:
							// exits cleanly if main times out or returns early
							return
						}
					}(ch)
				}

				if blockTimeout == 0 {
					// 0 means wait indefinitely
					<-merged
				} else {
					select {
					case <-merged:
					case <-time.After(blockTimeout):
					}
				}

				// Clean up registrations
				for i, k := range keys {
					removeStreamWaiter(k, waitChans[i])
				}

				// Try querying again after notification or timeout
				results, err = fetchEntries()
				if err != nil {
					resp := fmt.Sprintf("-%s\r\n", err.Error())
					_, _ = conn.Write([]byte(resp))
					continue
				}
			}

			if len(results) == 0 {
				// Nil array response on timeout or no match
				_, _ = conn.Write([]byte("*-1\r\n"))
				continue
			}

			// Serialize RESP array of streams
			var resp strings.Builder
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(results)))

			for _, res := range results {
				resp.WriteString("*2\r\n")
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(res.key), res.key))

				resp.WriteString(fmt.Sprintf("*%d\r\n", len(res.entries)))
				for _, entry := range res.entries {
					resp.WriteString("*2\r\n")
					resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID))

					resp.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))
					for _, f := range entry.Fields {
						resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(f.Key), f.Key))
						resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(f.Value), f.Value))
					}
				}
			}

			_, _ = conn.Write([]byte(resp.String()))

		case "INCR":

			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'incr' command"))
				continue
			}

			key := args[1]
			newVal, err := store.Incr(key)
			if err != nil {
				resp := fmt.Sprintf("-%s\r\n", err.Error())
				_, _ = conn.Write([]byte(resp))
				continue
			}

			resp := fmt.Sprintf(":%d\r\n", newVal)
			_, _ = conn.Write([]byte(resp))

		case "MULTI":

			if inTx {
				_, _ = conn.Write([]byte("-ERR Multi calls cannot be nested"))
				continue
			}
			inTx = true
			_, _ = conn.Write([]byte("+OK\r\n"))

		case "EXEC":
			if !inTx {
				_, _ = conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}

			// Execution of queued commands will be handled in the next stage
			inTx = false
			_, _ = conn.Write([]byte("*0\r\n"))

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
