package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"io"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379:", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection:", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	// Read the incoming PING command bytes from client
	buf := make([]byte, 1024)

	for {
		n, err := conn.Read(buf)

		if err != nil {

			if errors.Is(err, io.EOF) {
				fmt.Println("Client closed the connection")
				break
			}
			fmt.Println("Error reading from connection:", err.Error())
			break
		}
		if n == 0 {
			continue
		}

		// respond with +PONG\r\n for each received command
		_, err = conn.Write([]byte("+PONG\r\n"))

		if err != nil {
			fmt.Println("Error writing response:", err.Error())
			break
		}
	}
}