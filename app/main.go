package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"io"
)

func handleClient(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, 1024)

	// keep listening for successive commands from the client
	for {
		n, err := conn.Read(buf)

		if err != nil {
			if errors.Is(err, io.EOF) {
				// client cleanly closed the connection
				break
			}
			fmt.Println("Error reading from connection:", err.Error())
			break
		}
		if n == 0 {
			continue
		}


		// respond with +PONG\r\n for each command received
		_, err = conn.Write([]byte("+PONG\r\n"))

		if err != nil {
			fmt.Println("Error writing response:", err.Error())
			break
		}
	}

}

func main() {
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379:", err.Error())
		os.Exit(1)
	}
	defer l.Close()

	// outer loop: acccept multiple concurrent client connections
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to accept connection:", err.Error())
			continue
		}

		// spwan a goroutine for each client so they run concurrently
		go handleClient(conn)
	}
}