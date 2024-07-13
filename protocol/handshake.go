package protocol

import (
	"fmt"
	"io"
	"strconv"
)

// Handshake handles the handshake process from slave
func (s *Server) Handshake() error {
	err := sendPing(s.c)
	if err != nil {
		return fmt.Errorf("sendPing failed: %v", err)
	}

	response, err := s.c.GetLine()
	if err != nil {
		return fmt.Errorf("conn.GetLine() failed: %v", err)
	}

	if response != "+PONG" {
		return fmt.Errorf("Didn't receive \"PONG\": %s", response)
	}

	err = sendReplconf(s.c, s.opts.PortNum)
	if err != nil {
		return fmt.Errorf("sendReplconf failed: %v", err)
	}

	err = sendPsync(s.c)
	if err != nil {
		return fmt.Errorf("sendPsync failed: %v", err)
	}

	err = readRDB(s.c)
	if err != nil {
		return fmt.Errorf("readRDB failed: %v", err)
	}

	return nil
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

	full, err := c.GetLine()
	if err != nil {
		return fmt.Errorf("conn.GetLine failed: %v", err)
	}

	if full[:11] != "+FULLRESYNC" {
		return fmt.Errorf("Didn't recieve \"+FULLRESYNC\": %s", full)
	}

	return nil
}

func readRDB(c *Connection) error {
	token, err := c.GetLine()
	fmt.Println("Received RDB token:", token)
	if err != nil {
		return fmt.Errorf("conn.GetLine failed: %v", err)
	}

	if token[0] != '$' {
		return fmt.Errorf("Expected $, got %c", token[0])
	}

	rdbLen, err := strconv.Atoi(token[1:])
	fmt.Println("RDB length: ", rdbLen)
	if err != nil {
		return fmt.Errorf("Atoi failed: %v", err)
	}

	temp := make([]byte, rdbLen)
	rdbContent, err := io.ReadFull(c.reader, temp)
	if err != nil {
		return fmt.Errorf("ReadFull() failed: %v", err)
	}
	fmt.Println("Read RDB content length:", rdbContent)

	return nil
}
