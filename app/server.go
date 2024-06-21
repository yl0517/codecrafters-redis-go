package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		s, err := reader.ReadString('\n')
		if s[len(s)-1] == '\n' {
			s = s[:len(s)-1]
		}

		if s[len(s)-1] == '\r' {
			s = s[:len(s)-1]
		}

		if s == "PING" {
			sendPong(conn)
		}

		if err != nil {
			break
		}
	}

	os.Exit(0)
}

func sendPong(c net.Conn) {
	fmt.Println("pong")
	c.Write([]byte("+PONG\r\n"))
}
