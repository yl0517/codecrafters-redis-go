package protocol

import (
	"fmt"
	"net"
	"strings"
)

func Handshake(p string) {
	fmt.Print("yurr" + p)
	p2 := strings.Split(p, " ")
	addr := fmt.Sprintf("%s:%s", p2[0], p2[1])
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("net.Dial() failed: ", err.Error())
	}

	sendPing(conn)
}

func sendPing(c net.Conn) {
	c.Write([]byte("*1\r\n$4\r\nPING\r\n"))
}
