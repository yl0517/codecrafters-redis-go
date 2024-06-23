package protocol

import (
	"errors"
	"fmt"
)

var storage = map[string]string{}

// HandleRequest responds to the request recieved.
func HandleRequest(c *Connection, request []string) error {
	if request[0] == "PING" {
		err := handlePing(c)
		if err != nil {
			return fmt.Errorf("PING failed: %v", err)
		}
	}

	if request[0] == "ECHO" {
		err := handleEcho(c, request[1])
		if err != nil {
			return fmt.Errorf("ECHO failed: %v", err)
		}
	}

	if request[0] == "SET" {
		err := handleSet(c, request[1:])
		if err != nil {
			return fmt.Errorf("SET failed: %v", err)
		}
	}

	if request[0] == "GET" {
		err := handleGet(c, request[1])
		if err != nil {
			return fmt.Errorf("GET failed: %v", err)
		}
	}

	return nil
}

func handleEcho(c *Connection, message string) error {
	err := c.Write(fmt.Sprintf("$%d\r\n%s\r\n", len(message), message))
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}

func handlePing(c *Connection) error {
	err := c.Write("+PONG\r\n")
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}

func handleSet(c *Connection, request []string) error {
	if len(request) > 2 {
		return errors.New("Request is not a key value pair")
	}

	key := request[0]
	value := request[1]

	storage[key] = value

	err := c.Write("+OK\r\n")
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}

func handleGet(c *Connection, request string) error {
	err := c.Write(fmt.Sprintf("$%d\r\n%s\r\n", len(storage[request]), storage[request]))
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}
