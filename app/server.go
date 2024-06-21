package main

import (
	"fmt"
	// Uncomment this block to pass the first stage
	"bufio"
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
		_, err := reader.ReadString('\n')
		sendPong(conn)
		fmt.Println("testing")
		if err != nil {
			break
		}
	}

	os.Exit(0)
}

func sendPong(c net.Conn) {
	response := []byte("+PONG\r\n")
	c.Write(response)
}
