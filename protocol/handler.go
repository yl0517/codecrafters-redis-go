package protocol

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

var cache = map[string]*Entry{}

// Entry represents the cache entry.
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

// Server represents a server
type Server struct {
	Conn             *Connection
	RepInfo          string
	MasterReplid     string
	MasterReplOffset string
}

// HandleRequest responds to the request recieved.
func HandleRequest(server *Server, request []string, Repls map[string]*Connection) error {
	if request[0] == "PING" {
		err := handlePing(server.Conn)
		if err != nil {
			return fmt.Errorf("PING failed: %v", err)
		}
	}

	if request[0] == "ECHO" {
		err := handleEcho(server.Conn, request[1])
		if err != nil {
			return fmt.Errorf("ECHO failed: %v", err)
		}
	}

	if request[0] == "SET" {
		err := handleSet(server.Conn, request[1:])
		if err != nil {
			return fmt.Errorf("SET failed: %v", err)
		}
		handlePropagation(request, Repls)
	}

	if request[0] == "GET" {
		err := handleGet(server.Conn, request[1])
		if err != nil {
			return fmt.Errorf("GET failed: %v", err)
		}
	}

	if request[0] == "INFO" {
		err := handleInfo(request[1], server)
		if err != nil {
			return fmt.Errorf("GET failed: %v", err)
		}
	}

	if request[0] == "REPLCONF" {
		err := handleReplconf(server.Conn)
		if err != nil {
			return fmt.Errorf("REPLCONF failed: %v", err)
		}
		remoteAddr, conn := server.Conn.conn.RemoteAddr().String(), server.Conn
		Repls[remoteAddr] = conn
	}

	if request[0] == "PSYNC" {
		err := handlePsync(request[1:], server)
		if err != nil {
			return fmt.Errorf("REPLCONF failed: %v", err)
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

func handleInfo(arg string, server *Server) error {
	var s string

	if arg == "replication" {
		s += "# Replication\r\n"
		if server.RepInfo != "" {
			s += "role:slave\r\n"
		} else {
			s += "role:master\r\n"
		}

		s += fmt.Sprintf("master_replid:%s\r\n", server.MasterReplid)

		s += fmt.Sprintf("master_repl_offset:%s\r\n", server.MasterReplOffset)
	}

	err := server.Conn.Write(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}
	return nil
}

func handleReplconf(c *Connection) error {
	err := c.Write("+OK\r\n")
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}

func handlePsync(request []string, server *Server) error {
	emptyRDB, err1 := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
	if err1 != nil {
		return fmt.Errorf("DecodeString failed: %v", err1)
	}

	if request[0] == "?" {
		err2 := server.Conn.Write(fmt.Sprintf("+FULLRESYNC %s 0\r\n", server.MasterReplid))
		if err2 != nil {
			return fmt.Errorf("Write failed: %v", err2)
		}
	}

	err := server.Conn.Write(fmt.Sprintf("$%d\r\n%s", len(string(emptyRDB)), string(emptyRDB)))
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}

func handlePropagation(command []string, Repls map[string]*Connection) error {
	propCmd := ToRespArray(command)
	fmt.Print(propCmd)

	for _, conn := range Repls {
		err := conn.Write(propCmd)
		if err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
	}

	return nil
}
