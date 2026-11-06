package main

import (
	"flag"
	"fmt"
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
