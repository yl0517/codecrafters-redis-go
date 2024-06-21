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

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}

}

func handleConnection(c net.Conn) {
	defer c.Close()

	reader := bufio.NewReader(c)

	for {
		s, err := reader.ReadString('\n')

		if len(s) > 0 {
			if s[len(s)-1] == '\n' {
				s = s[:len(s)-1]
			}

			if s[len(s)-1] == '\r' {
				s = s[:len(s)-1]
			}

			if s == "PING" {
				sendPong(c)
			}
		}

		if err != nil {
			break
		}
	}
}

func sendPong(c net.Conn) {
	fmt.Println("pong")
	c.Write([]byte("+PONG\r\n"))
}
