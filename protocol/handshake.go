package protocol

import (
	"fmt"
	"net"
	"strings"
)

func Handshake(rep string, p string) {
	repArr := strings.Split(rep, " ")
	addr := fmt.Sprintf("%s:%s", repArr[0], repArr[1])
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("net.Dial() failed: ", err.Error())
	}

	c := NewConnection(conn)

	sendPing(c)

	respCounter := 0
	for {
		response, err := c.GetLine()
		if err != nil {
			fmt.Println("conn.GetLine() failed: ", err.Error())
			return
		}
		respCounter++

		if response == "+PONG" {
			sendReplconf(c, p)
		}

		if respCounter >= 3 {
			sendPsync(c)
		}
	}
}

func sendPing(c *Connection) {
	c.Write("*1\r\n$4\r\nPING\r\n")
}

func sendReplconf(c *Connection, port string) {
	c.Write(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(port), port))
	c.Write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
}

func sendPsync(c *Connection) {
	c.Write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
}
