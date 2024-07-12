package protocol

import (
	"fmt"
)

// Handshake handles the handshake process from slave
func Handshake(c *Connection, o Opts) {
	err := sendPing(c)
	if err != nil {
		fmt.Println("sendPing failed: ", err.Error())
		return
	}

	response, err := c.GetLine()
	if err != nil {
		fmt.Println("conn.GetLine() failed: ", err.Error())
		return
	}

	if response != "+PONG" {
		fmt.Println("Didn't recieve \"PONG\": ", response)
	}

	err = sendReplconf(c, o.PortNum)
	if err != nil {
		fmt.Println("sendReplconf failed: ", err.Error())
		return
	}

	sendPsync(c)
}

func sendPing(c *Connection) error {
	err := c.Write("*1\r\n$4\r\nPING\r\n")
	if err != nil {
		return fmt.Errorf("c.Write failed: %v", err)
	}
	return nil
}

func sendReplconf(c *Connection, port string) error {
	c.Write(fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n", len(port), port))
	ok, err := c.GetLine()
	if err != nil {
		return fmt.Errorf("conn.GetLine failed: %v", err)
	}

	if ok != "+OK" {
		return fmt.Errorf("Didn't recieve \"OK\": %s", ok)
	}

	err = c.Write("*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
	if err != nil {
		return fmt.Errorf("c.Write failed: %v", err)
	}

	ok, err = c.GetLine()
	if err != nil {
		return fmt.Errorf("conn.GetLine failed: %v", err)
	}

	if ok != "+OK" {
		return fmt.Errorf("Didn't recieve \"OK\": %s", ok)
	}

	return nil
}

func sendPsync(c *Connection) error {
	err := c.Write("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
	if err != nil {
		return fmt.Errorf("c.Write failed: %v", err)
	}
	return nil
}
