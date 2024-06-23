package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/protocol"
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
	conn := protocol.NewConnection(c)

	defer conn.Close()

	for {
		numElem, err := conn.GetLine()
		if err != nil {
			fmt.Println("conn.GetLine() failed: ", err.Error())
			return
		}

		len, err := protocol.GetArrayLength(numElem)
		if err != nil {
			fmt.Println("protocol.GetLength() failed: ", err.Error())
			return
		}

		var request []string

		for i := 0; i < len; i++ {
			line, err := conn.GetLine()
			if err != nil {
				fmt.Println("conn.GetLine() failed: ", err.Error())
				return
			}

			len, err := protocol.GetBulkStringLength(line)
			if err != nil {
				fmt.Println("protocol.GetBulkStringLength() failed: ", err.Error())
				return
			}

			s, err := conn.GetLine()
			if err != nil {
				fmt.Println("conn.GetLine() failed: ", err.Error())
				return
			}

			err = protocol.VerifyBulkStringLength(s, len)
			if err != nil {
				fmt.Println("protocol.VerifyBulkStringLength() failed: ", err.Error())
				return
			}

			request = append(request, s)
		}

		handleRequest(c, request)

		if err != nil {
			break
		}
	}
}

func handleRequest(c net.Conn, request []string) {
	if request[0] == "PING" {
		handlePing(c)
	}

	if request[0] == "ECHO" {
		handleEcho(c, request[1])
	}
}

func handleEcho(c net.Conn, message string) {
	c.Write([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)))
}

func handlePing(c net.Conn) {
	fmt.Println("pong")
	c.Write([]byte("+PONG\r\n"))
}
