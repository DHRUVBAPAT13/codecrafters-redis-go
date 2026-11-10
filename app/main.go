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

	case "PUBLISH":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'publish' command\r\n")
		}

		channel := args[1]
		message := args[2]
		count := pubsubManager.Publish(channel, message)
		return fmt.Appendf(nil, ":%d\r\n", count)

	case "ZADD":
		if len(args) < 4 {
			return []byte("-ERR wrong number of arguments for 'zadd' command\r\n")
		}

		key := args[1]
		score, err := strconv.ParseFloat(args[2], 64)
		if err != nil {
			return []byte("-ERR value is not a valid float\r\n")
		}
		member := args[3]

		added := store.ZAdd(key, score, member)
		watchManager.TouchKey(key)

		// also append to AOF if enabled
		rawCmd := EncodeRESPArray(args)
		_ = aofManager.AppendCommand(rawCmd)

		if config.Role == "master" {
			replicaRegistry.Propagate(rawCmd)
		}

		return fmt.Appendf(nil, ":%d\r\n", added)

	case "ZRANK":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'zrank' command\r\n")
		}

		key := args[1]
		member := args[2]

		rank, exists := store.ZRank(key, member)
		if !exists {
			return []byte("$-1\r\n")
		}

		return fmt.Appendf(nil, ":%d\r\n", rank)

	case "ZRANGE":
		if len(args) < 4 {
			return []byte("-ERR wrong number of arguments for 'zrange' command\r\n")
		}

		key := args[1]
		start, err1 := strconv.Atoi(args[2])
		stop, err2 := strconv.Atoi(args[3])

		if err1 != nil || err2 != nil {
			return []byte("-ERR value is not an intger or out of range\r\n")
		}

		members := store.ZRange(key, start, stop)

		var resp strings.Builder
		resp.WriteString(fmt.Sprintf("*%d\r\n", len(members)))

		for _, m := range members {
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(m), m))
		}

		return []byte(resp.String())

	case "ZCARD":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'zcard' command\r\n")
		}

		key := args[1]
		count := store.ZCard(key)
		return fmt.Appendf(nil, ":%d\r\n", count)

	case "ZSCORE":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'zscore' command\r\n")
		}

		key := args[1]
		member := args[2]

		score, exists := store.ZScore(key, member)
		if !exists {
			return []byte("$-1\r\n")
		}

		scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
		return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(scoreStr), scoreStr)

	case "ZREM":
		if len(args) < 3 {
			return []byte("-ERR wrong arguments for 'zrem' command\r\n")
		}

		key := args[1]
		removed := store.ZRem(key, args[2:]...)
		watchManager.TouchKey(key)

		rawCmd := EncodeRESPArray(args)
		_ = aofManager.AppendCommand(rawCmd)
		if config.Role == "master" {
			replicaRegistry.Propagate(rawCmd)
		}

		return fmt.Appendf(nil, ":%d\r\n", removed)

	case "SETBIT":
		if len(args) < 4 {
			return []byte("-ERR wrong number of arguments for 'setbit' command\r\n")
		}

		key := args[1]
		offset, err := strconv.Atoi(args[2])
		if err != nil || offset < 0 {
			return []byte("-ERR bit offset is not an integer or out of range\r\n")
		}

		bit, err := strconv.Atoi(args[3])
		if err != nil || (bit != 0 && bit != 1) {
			return []byte("-ERR bit is not the integer 0 or 1\r\n")
		}

		origBit, err := store.SetBit(key, offset, bit)
		if err != nil {
			return fmt.Appendf(nil, "-%s\r\n", err.Error())
		}

		watchManager.TouchKey(key)

		rawCmd := EncodeRESPArray(args)
		_ = aofManager.AppendCommand(rawCmd)
		if config.Role == "master" {
			replicaRegistry.Propagate(rawCmd)
		}

		return fmt.Appendf(nil, ":%d\r\n", origBit)

	case "GETBIT":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'getbit' command\r\n")
		}

		key := args[1]
		offset, err := strconv.Atoi(args[2])
		if err != nil || offset < 0 {
			return []byte("-ERR bit offset is not an integer or out of range\r\n")
		}

		bit := store.GetBit(key, offset)
		return fmt.Appendf(nil, ":%d\r\n", bit)

	case "STRLEN":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'strlen' command\r\n")
		}

		key := args[1]
		length := store.StrLen(key)
		return fmt.Appendf(nil, ":%d\r\n", length)

	case "BITCOUNT":
		if len(args) < 2 {
			return []byte("-ERR wrong number arguments for 'bitcount' command\r\n")
		}

		key := args[1]
		hasRange := false
		start := 0
		end := 0

		if len(args) >= 4 {
			var err1, err2 error
			start, err1 = strconv.Atoi(args[2])
			end, err2 = strconv.Atoi(args[3])

			if err1 != nil || err2 != nil {
				return []byte("-ERR value is not an integer or out of range\r\n")
			}
			hasRange = true
		} else if len(args) == 3 {
			return []byte("-ERR syntax error\r\n")
		}

		total := store.BitCount(key, hasRange, start, end)
		return fmt.Appendf(nil, ":%d\r\n", total)

	case "BITOP":
		if len(args) < 4 {
			return []byte("-ERR wrong number of arguments for 'bitop' command\r\n")
		}

		op := args[1]
		destKey := args[2]
		srcKeys := args[3:]

		length, err := store.BitOp(op, destKey, srcKeys...)
		if err != nil {
			return fmt.Appendf(nil, "-%s\r\n", err.Error())
		}

		watchManager.TouchKey(destKey)

		rawCmd := EncodeRESPArray(args)
		_ = aofManager.AppendCommand(rawCmd)
		if config.Role == "master" {
			replicaRegistry.Propagate(rawCmd)
		}

		return fmt.Appendf(nil, ":%d\r\n", length)

	case "GEOADD":
		if len(args) < 5 || (len(args)-2)%3 != 0 {
			return []byte("-ERR wrong number of arguments for 'geoadd' command\r\n")
		}

		const (
			minLon = -180.0
			maxLon = 180.0
			minLat = -85.05112878
			maxLat = 85.05112878
		)

		key := args[1]

		// 1. validate all coordinate points
		type geoItem struct {
			lon    float64
			lat    float64
			member string
		}
		items := make([]geoItem, 0, (len(args)-2)/3)

		for i := 2; i < len(args); i += 3 {

			lonStr := args[i]
			latStr := args[i+1]
			member := args[i+2]

			lon, errLon := strconv.ParseFloat(lonStr, 64)
			lat, errLat := strconv.ParseFloat(latStr, 64)

			if errLon != nil || errLat != nil || lon < geoMinLon || lat < geoMinLat || lon > geoMaxLon || lat > geoMaxLat {
				return fmt.Appendf(nil, "-ERR invalid longitude, latitude pair %s,%s\r\n", lonStr, latStr)
			}

			items = append(items, geoItem{lon: lon, lat: lat, member: member})
		}

		// 2. insert into sorted set with calculated score
		addedCount := 0
		for _, item := range items {

			score := float64(encodeGeo(item.lat, item.lon))
			addedCount += store.ZAdd(key, score, item.member)
		}

		watchManager.TouchKey(key)

		rawCmd := EncodeRESPArray(args)
		_ = aofManager.AppendCommand(rawCmd)

		if config.Role == "master" {
			replicaRegistry.Propagate(rawCmd)
		}

		return fmt.Appendf(nil, ":%d\r\n", addedCount)

	case "GEOPOS":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'geopos' command\r\n")
		}

		key := args[1]
		members := args[2:]

		var resp strings.Builder
		resp.WriteString(fmt.Sprintf("*%d\r\n", len(members)))

		for _, member := range members {
			score, exists := store.ZScore(key, member)
			if !exists {
				resp.WriteString("*-1\r\n")
				continue
			}

			coords := decodeGeo(uint64(score))
			lonStr := strconv.FormatFloat(coords.Longitude, 'f', -1, 64)
			latStr := strconv.FormatFloat(coords.Latitude, 'f', -1, 64)

			resp.WriteString("*2\r\n")
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(lonStr), lonStr))
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(latStr), latStr))

		}

		return []byte(resp.String())

	case "GEODIST":
		if len(args) < 4 {
			return []byte("-ERR wrong number of arguments for 'geodist' command\r\n")
		}

		key := args[1]
		member1 := args[2]
		member2 := args[3]

		score1, exists1 := store.ZScore(key, member1)
		score2, exists2 := store.ZScore(key, member2)

		if !exists1 || !exists2 {
			return []byte("$-1\r\n")
		}

		coord1 := decodeGeo(uint64(score1))
		coord2 := decodeGeo(uint64(score2))

		dist := haversineDistance(coord1, coord2)
		distStr := fmt.Sprintf("%.4f", dist)

		return fmt.Appendf(nil, "$%d\r\n%s\r\n", len(distStr), distStr)

	case "GEOSEARCH":
		if len(args) < 7 {
			return []byte("-ERR wrong number of arguments for 'geosearch' command\r\n")
		}

		key := args[1]
		var centerLon, centerLat float64
		var radius float64
		var unit string

		hasFromLonLat := false
		hasByRadius := false

		for i := 2; i < len(args); i++ {
			token := strings.ToUpper(args[i])

			switch token {
			case "FROMLONLAT":

				var err1, err2 error
				centerLon, err1 = strconv.ParseFloat(args[i+1], 64)
				centerLat, err2 = strconv.ParseFloat(args[i+2], 64)

				if err1 != nil || err2 != nil {
					return []byte("-ERR invalid longitude, latitude pair\r\n")
				}
				hasFromLonLat = true
				i += 2

			case "BYRADIUS":
				if i+2 >= len(args) {
					return []byte("-ERR synatx error\r\n")
				}
				var err error
				radius, err = strconv.ParseFloat(args[i+1], 64)

				if err != nil || radius < 0 {
					return []byte("-ERR value is not a valid float\r\n")
				}
				unit = args[i+2]
				hasByRadius = true
				i += 2
			}
		}

		if !hasByRadius || !hasFromLonLat {
			return []byte("-ERR syntax error\r\n")
		}

		radiusMeters, err := parseRadiusToMeters(radius, unit)
		if err != nil {
			return []byte("-ERR syntax error\r\n")
		}

		members := store.GetAllZSetMembers(key)
		if len(members) == 0 {
			return []byte("*0\r\n")
		}

		centerCoord := Coordinates{
			Latitude:  centerLat,
			Longitude: centerLon,
		}

		var matched []string
		for member, score := range members {

			memberCoord := decodeGeo(uint64(score))
			dist := haversineDistance(centerCoord, memberCoord)

			if dist <= radiusMeters {
				matched = append(matched, member)
			}
		}

		var resp strings.Builder
		resp.WriteString(fmt.Sprintf("*%d\r\n", len(matched)))

		for _, m := range matched {
			resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(m), m))
		}

		return []byte(resp.String())

	case "ACL":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'acl' command\r\n")
		}

		subCmd := strings.ToUpper(args[1])

		switch subCmd {
		case "WHOAMI":
			return []byte("$7\r\ndefault\r\n")

		case "GETUSER":
			if len(args) < 3 {
				return []byte("-ERR wrong number of arguments for 'acl|getuser' command\r\n")
			}

			username := args[2]
			user, exists := aclManager.GetUser(username)
			if !exists {
				return []byte("$-1\r\n")
			}

			// 1. flags
			var resp strings.Builder
			resp.WriteString("*4\r\n")

			resp.WriteString("$5\r\nflags\r\n")
			if user.NoPass {
				resp.WriteString("*1\r\n$6\r\nnopass\r\n")
			} else {
				resp.WriteString("*0\r\n")
			}

			// 2. password
			resp.WriteString("$9\r\npasswords\r\n")
			resp.WriteString(fmt.Sprintf("*%d\r\n", len(user.Passwords)))
			for _, passHash := range user.Passwords {
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(passHash), passHash))
			}
			return []byte(resp.String())

		case "SETUSER":
			if len(args) < 3 {
				return []byte("-ERR wrong number of arguments for 'acl|setuser' command\r\n")
			}

			username := args[2]
			rules := args[3:]
			_ = aclManager.SetUser(username, rules)

			return []byte("+OK\r\n")

		default:
			return fmt.Appendf(nil, "-ERR unknown subcommand '%s'. Try ACL HELP.\r\n", strings.ToUpper(args[1]))
		}

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
		// clean up any channel subscriptions for this connection
		pubsubManager.UnsubscribeAll(conn)
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

		// Subscribed mode check: if client is subscribed to one or more channels,
		// only specific pub/sub management commands, PING, QUIT, and RESET are permitted.
		if len(txState.SubscribedChannels) > 0 {
			switch cmd {
			case "PING":
				// If optional message argument provided (e.g., PING message), Redis echoes that message in index 2.
				// By default, it returns ["pong", ""]
				msg := ""
				if len(args) > 1 {
					msg = args[1]
				}
				resp := fmt.Sprintf("*2\r\n$4\r\npong\r\n$%d\r\n%s\r\n", len(msg), msg)
				_, _ = conn.Write([]byte(resp))
				continue
			case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "QUIT", "RESET":
				// Allowed commands in subscribed mode
			default:
				errMsg := fmt.Sprintf("-ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context\r\n", strings.ToLower(args[0]))
				_, _ = conn.Write([]byte(errMsg))
				continue
			}
		}

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

		case "SUBSCRIBE":
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'subscribe' command\r\n"))
				continue
			}
			var resp strings.Builder
			for _, channel := range args[1:] {
				txState.SubscribedChannels[channel] = struct{}{}
				pubsubManager.Subscribe(channel, conn)

				count := len(txState.SubscribedChannels)
				resp.WriteString("*3\r\n")
				resp.WriteString("$9\r\nsubscribe\r\n")
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(channel), channel))
				resp.WriteString(fmt.Sprintf(":%d\r\n", count))
			}
			_, _ = conn.Write([]byte(resp.String()))
			continue

		case "UNSUBSCRIBE":
			channels := args[1:]
			// If no channel names provided, unsubscribe from all channels the client is subscribed to
			if len(channels) == 0 {
				for ch := range txState.SubscribedChannels {
					channels = append(channels, ch)
				}
			}

			// If client wasn't subscribed to any channel and sent bare UNSUBSCRIBE
			if len(channels) == 0 {
				resp := "*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
				_, _ = conn.Write([]byte(resp))
				continue
			}

			var resp strings.Builder
			for _, channel := range channels {
				delete(txState.SubscribedChannels, channel)
				pubsubManager.Unsubscribe(channel, conn)

				remainingCount := len(txState.SubscribedChannels)
				resp.WriteString("*3\r\n")
				resp.WriteString("$11\r\nunsubscribe\r\n")
				resp.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(channel), channel))
				resp.WriteString(fmt.Sprintf(":%d\r\n", remainingCount))
			}
			_, _ = conn.Write([]byte(resp.String()))
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
