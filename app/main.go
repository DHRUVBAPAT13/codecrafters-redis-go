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

func executeCommand(args []string, store *Store, config ServerConfig) []byte {
	if len(args) == 0 {
		return nil
	}

	cmd := strings.ToUpper(args[0])

	switch cmd {

	case "INFO":
		section := ""
		if len(args) > 1 {
			section = strings.ToLower(args[1])
		}

		var infoContent string
		if section == "replication" || section == "" {
			infoContent = config.GetReplicationInfo()
		}
		return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(infoContent), infoContent)

	case "PING":
		return []byte("+PONG\r\n")

	case "ECHO":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'echo' command\r\n")
		}
		msg := args[1]
		return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(msg), msg)

	case "SET":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'set' command\r\n")
		}
		key := args[1]
		val := args[2]

		var expiry time.Duration
		for i := 3; i < len(args); i++ {
			if strings.ToUpper(args[i]) == "PX" && i+1 < len(args) {
				ms, err := strconv.ParseInt(args[i+1], 10, 64)
				if err == nil {
					expiry = time.Duration(ms) * time.Millisecond
				}
				i++
			}
		}

		store.Set(key, val, expiry)
		watchManager.TouchKey(key)

		rawCmd := EncodeRESPArray(args)

		// 1. write to AOF and fsync if required
		_ = aofManager.AppendCommand(rawCmd)

		// 2. propagate to replicas if running as master
		if config.Role == "master" {
			replicaRegistry.Propagate(rawCmd)
		}

		return []byte("+OK\r\n")

	case "GET":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'get' command\r\n")
		}
		key := args[1]
		val, ok := store.Get(key)
		if !ok {
			return []byte("$-1\r\n")
		}
		return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(val), val)

	case "TYPE":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'type' command\r\n")
		}
		return fmt.Appendf(nil, "+%s\r\n", store.Type(args[1]))

	case "INCR":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'incr' command\r\n")
		}
		key := args[1]
		newVal, err := store.Incr(key)
		if err != nil {
			return fmt.Appendf(nil, "-%s\r\n", err.Error())
		}
		watchManager.TouchKey(key)
		return fmt.Appendf(nil, ":%d\r\n", newVal)

	case "RPUSH":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'rpush' command\r\n")
		}
		key := args[1]
		length := store.RPush(key, args[2:]...)
		watchManager.TouchKey(key)
		return fmt.Appendf(nil, ":%d\r\n", length)

	case "LPUSH":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'lpush' command\r\n")
		}
		key := args[1]
		length := store.LPush(key, args[2:]...)
		watchManager.TouchKey(key)
		return fmt.Appendf(nil, ":%d\r\n", length)

	case "LPOP":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'lpop' command\r\n")
		}
		key := args[1]
		count := 1
		if len(args) > 2 {
			c, err := strconv.Atoi(args[2])
			if err != nil || c < 0 {
				return []byte("-ERR value is not an integer or out of range\r\n")
			}
			count = c
		}

		vals, ok := store.LPop(key, count)
		if !ok || len(vals) == 0 {
			return []byte("$-1\r\n")
		}

		watchManager.TouchKey(key)

		if len(args) == 2 {
			return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(vals[0]), vals[0])
		}

		var b strings.Builder
		b.WriteString(fmt.Sprintf("*%d\r\n", len(vals)))
		for _, v := range vals {
			b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
		}
		return []byte(b.String())

	case "LLEN":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'llen' command\r\n")
		}
		key := args[1]
		return fmt.Appendf(nil, ":%d\r\n", store.LLen(key))

	case "LRANGE":
		if len(args) < 4 {
			return []byte("-ERR wrong number of arguments for 'lrange' command\r\n")
		}
		key := args[1]
		start, _ := strconv.Atoi(args[2])
		stop, _ := strconv.Atoi(args[3])
		items := store.LRange(key, start, stop)

		var b strings.Builder
		b.WriteString(fmt.Sprintf("*%d\r\n", len(items)))
		for _, item := range items {
			b.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(item), item))
		}
		return []byte(b.String())

	case "XADD":
		if len(args) < 4 || (len(args)-3)%2 != 0 {
			return []byte("-ERR wrong number of arguments for 'xadd' command\r\n")
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
			return fmt.Appendf(nil, "-%s\r\n", err.Error())
		}
		return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(entryID), entryID)

	case "XRANGE":
		if len(args) < 4 {
			return []byte("-ERR wrong number of arguments for 'xrange' command\r\n")
		}
		key := args[1]
		startBound := args[2]
		endBound := args[3]

		entries, err := store.XRange(key, startBound, endBound)
		if err != nil {
			return fmt.Appendf(nil, "-%s\r\n", err.Error())
		}

		var resp strings.Builder
		resp.WriteString(fmt.Sprintf("*%d\r\n", len(entries)))
		for _, entry := range entries {

			resp.WriteString("*2\r\n")
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.ID), entry.ID))
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(entry.Fields)*2))

			for _, f := range entry.Fields {
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(f.Key), f.Key))
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(f.Value), f.Value))
			}
		}
		return []byte(resp.String())

	case "UNWATCH":
		return []byte("+OK\r\n")

	case "REPLCONF":
		return []byte("+OK\r\n")

	case "WAIT":
		if len(args) > 3 {
			return []byte("ERR wrong number of arguments for 'wait' command\r\n")
		}

		numReplicas, err1 := strconv.Atoi(args[1])
		timeoutMs, err2 := strconv.Atoi(args[2])

		if err1 != nil || err2 != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}

		// when no replicas are connected or waiting for 0 replicas, return the connected count immediately
		count := replicaRegistry.WaitForReplicas(numReplicas, timeoutMs)
		return fmt.Appendf(nil, ":%d\r\n", count)

	case "CONFIG":
		if len(args) < 3 || strings.ToUpper(args[1]) != "GET" {
			return []byte("-ERR syntax error\r\n")
		}

		param := strings.ToLower(args[2])
		val := ""

		switch param {
		case "dir":
			val = config.Dir

		case "dbfilename":
			val = config.DBFileName

		case "appendonly":
			val = config.AppendOnly

		case "appenddirname":
			val = config.AppendDirName

		case "appendfilename":
			val = config.AppendFileName

		case "appendfsync":
			val = config.AppendFsync

		default:
			// if unknown parameter, Redis returns an empty array
			return []byte("*0\r\n")
		}

		// Return a 2-element RESP Array: [param, val]
		return fmt.Appendf(nil, "*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(param), param, len(val), val)

	case "KEYS":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'keys' command\r\n")
		}

		pattern := args[1]
		var matchedKeys []string
		if pattern == "*" {
			matchedKeys = store.Keys()
		}

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("*%d\r\n", len(matchedKeys)))
		for _, k := range matchedKeys {
			sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
		}
		return []byte(sb.String())

	default:
		return fmt.Appendf(nil, "-ERR unknown command '%s'\r\n", cmd)
	}
}

func handleBLPOP(conn net.Conn, store *Store, args []string) {
	if len(args) < 3 {
		_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'blpop' command\r\n"))
		return
	}

	key := args[1]
	timeoutSec, err := strconv.ParseFloat(args[2], 64)
	if err != nil || timeoutSec < 0 {
		_, _ = conn.Write([]byte("-ERR timeout is not a float or out of range\r\n"))
		return
	}

	timeout := time.Duration(timeoutSec * float64(time.Second))
	val, ok := store.BLPop(key, timeout)
	if !ok {
		_, _ = conn.Write([]byte("*-1\r\n"))
		return
	}

	watchManager.TouchKey(key)

	resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(val), val)
	_, _ = conn.Write([]byte(resp))
}

func handleXREAD(conn net.Conn, store *Store, args []string) {
	var blockTimeout time.Duration
	isBlock := false

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
		return
	}

	streamArgs := args[streamsIdx+1:]
	if len(streamArgs)%2 != 0 {
		_, _ = conn.Write([]byte("-ERR syntax error, unmatched keys and IDs\r\n"))
		return
	}

	numStreams := len(streamArgs) / 2
	keys := streamArgs[:numStreams]
	ids := streamArgs[numStreams:]

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
		return
	}

	if len(results) == 0 && isBlock {
		waitChans := make([]chan struct{}, len(keys))
		for i, k := range keys {
			waitChans[i] = registerStreamWaiter(k)
		}

		merged := make(chan struct{}, 1)
		done := make(chan struct{})

		for _, ch := range waitChans {
			go func(c chan struct{}) {
				select {
				case <-c:
					select {
					case merged <- struct{}{}:
					default:
					}
				case <-done:
					return
				}
			}(ch)
		}

		if blockTimeout == 0 {
			<-merged
		} else {
			select {
			case <-merged:
			case <-time.After(blockTimeout):
			}
		}

		close(done)

		for i, k := range keys {
			removeStreamWaiter(k, waitChans[i])
		}

		results, err = fetchEntries()
		if err != nil {
			resp := fmt.Sprintf("-%s\r\n", err.Error())
			_, _ = conn.Write([]byte(resp))
			return
		}
	}

	if len(results) == 0 {
		_, _ = conn.Write([]byte("*-1\r\n"))
		return
	}

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
}

func handleClient(conn net.Conn, store *Store, config ServerConfig) {
	isReplica := false
	defer func() {
		if !isReplica {
			conn.Close()
		}
	}()

	reader := bufio.NewReader(conn)
	txState := NewClientTxState()

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

		// 1. Transaction Interceptor:
		if txState.InTx {
			switch cmd {
			case "MULTI":
				_, _ = conn.Write([]byte("-ERR MULTI calls can not be nested\r\n"))
				continue

			case "WATCH":
				_, _ = conn.Write(txState.HandleWatch(args))
				continue

			case "EXEC", "DISCARD":
				// fall through to execution below

			default:
				txState.Queue = append(txState.Queue, args)
				_, _ = conn.Write([]byte("+QUEUED\r\n"))
				continue
			}
		}

		// 2. Command routing outside of tx or transaction control
		switch cmd {

		case "WATCH":
			_, _ = conn.Write(txState.HandleWatch(args))
			continue

		case "UNWATCH":
			_, _ = conn.Write(txState.HandleUnwatch())
			continue

		case "MULTI":
			_, _ = conn.Write(txState.HandleMulti())
			continue

		case "EXEC":
			if !txState.InTx {
				_, _ = conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}

			if txState.IsDirty() {
				txState.Reset()
				_, _ = conn.Write([]byte("*-1\r\n"))
				continue
			}

			if len(txState.Queue) == 0 {
				_, _ = conn.Write([]byte("*0\r\n"))
				txState.Reset()
				continue
			}

			var resp strings.Builder
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(txState.Queue)))

			for _, queueCmd := range txState.Queue {
				out := executeCommand(queueCmd, store, config)
				resp.Write(out)
			}

			txState.Reset()
			_, _ = conn.Write([]byte(resp.String()))
			continue

		case "DISCARD":
			if !txState.InTx {
				_, _ = conn.Write([]byte("-ERR DISCARD without MULTI\r\n"))
				continue
			}
			txState.Reset()
			_, _ = conn.Write([]byte("+OK\r\n"))
			continue

		case "PSYNC":
			if err := HandleMasterPSYNC(conn, reader, config); err != nil {
				fmt.Println("Error handling PSYNC:", err.Error())
				return
			}
			// Mark as replica so defer does not close this socket!
			isReplica = true
			return

		case "BLPOP":
			handleBLPOP(conn, store, args)
			continue

		case "XREAD":
			handleXREAD(conn, store, args)
			continue
		}

		out := executeCommand(args, store, config)
		if out != nil {
			_, _ = conn.Write(out)
		}
	}
}

func main() {
	config := ParseServerConfig()
	store := NewStore()

	// initialize AOF persistence if enabled
	if err := InitAOF(config, store); err != nil {
		fmt.Printf("AOF init error: %s\n", err.Error())
		os.Exit(1)
	}

	// Load RDB snapshot into store if present
	if err := LoadRDB(config.Dir, config.DBFileName, store); err != nil {
		fmt.Println("Error loading RDB file:", err.Error())
	}

	if config.Role == "slave" {
		go StartReplicationHandshake(config, store)
	}

	l, err := net.Listen("tcp", config.Address())
	if err != nil {
		fmt.Printf("Failed to bind to port %d: %s\n", config.Port, err.Error())
		os.Exit(1)
	}
	defer l.Close()

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}

		go handleClient(conn, store, config)
	}
}
