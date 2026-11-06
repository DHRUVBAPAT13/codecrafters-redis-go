package main

import (
	"flag"
	"fmt"
	"strings"
)

type ServerConfig struct {
	Port int
	Role string
}

func ParseServerConfig() ServerConfig {
	port := flag.Int("port", 6379, "Port to listen to")
	flag.Parse()

	return ServerConfig{
		Port: *port,
		Role: "master",
	}
}

func (c ServerConfig) Address() string {
	return fmt.Sprintf("0.0.0.0:%d", c.Port)
}

func (c ServerConfig) GetReplicationInfo() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("role:%s\r\n", c.Role))
	return sb.String()
}
