package protocol

import (
	"fmt"
	"strconv"
	"time"
)

var cache = map[string]*Entry{}

// Entry represents the cache entry.ß
type Entry struct {
	msg      string
	expireAt int64
}

// NewEntry is the Entry constructor.
func NewEntry(s string, t int64) *Entry {
	return &Entry{
		msg:      s,
		expireAt: t,
	}
}

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

	if request[0] == "INFO" {
		err := handleInfo(c, request[1])
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
	key := request[0]
	value := request[1]

	var expireAt int64 // initially zero
	if len(request) == 4 {
		expireAfter, err := strconv.ParseInt(request[3], 10, 64)
		if err != nil {
			return fmt.Errorf("Atoi failed: %v", err)
		}
		expireAt = time.Now().UnixMilli() + expireAfter
	}

	cache[key] = NewEntry(value, expireAt)

	err := c.Write("+OK\r\n")
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}

func handleGet(c *Connection, key string) error {
	now := time.Now().UnixMilli()

	entry, ok := cache[key]
	if !ok {
		err := c.Write("$-1\r\n")
		if err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
		return nil
	}

	if entry.expireAt != 0 && now > entry.expireAt {
		err := c.Write("$-1\r\n")
		if err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
		delete(cache, key)
		return nil
	}

	err := c.Write(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.msg), entry.msg))
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}

func handleInfo(c *Connection, arg string) error {
	if arg == "replication" {
		err := c.Write("$11\r\nrole:master\r\n")
		if err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
	}
	return nil
}
