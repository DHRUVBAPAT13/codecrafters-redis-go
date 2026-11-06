package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type ServerConfig struct {
	Port       int
	Role       string
	MasterHost string
	MasterPort int
}

func ParseServerConfig() ServerConfig {
	port := flag.Int("port", 6379, "Port to listen to")
	replicaOf := flag.String("replicaof", "", "Master host and portto replicate it")
	flag.Parse()

	config := ServerConfig{
		Port: *port,
		Role: "master",
	}

	// Handle case where flag.Parse might receive replicaof as flag or separate args
	var replicaArg string
	if *replicaOf != "" {
		replicaArg = *replicaOf
	} else {
		// Check for --replicaof in os.Args if passed as two separate values (e.g. --replicaof localhost 6379)
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
	return sb.String()
}
