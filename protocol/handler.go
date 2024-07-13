package protocol

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
)

// Server represents a server
type Server struct {
	c       *Connection
	opts    Opts
	storage *Storage
}

// NewServer is the server constructor
func NewServer(conn *Connection, o Opts) *Server {
	return &Server{
		c:       conn,
		opts:    o,
		storage: NewStorage(),
	}
}

func (s *Server) Handle() {
	defer s.c.Close()

	for {
		request, err := s.c.Read()
		if err != nil {
			fmt.Printf("conn.Read() failed: %v\n", err)
			return
		}

		err = HandleRequest(s, request)
		if err != nil {
			fmt.Printf("protocol.HandleRequest() failed: %v\n", err)
		}
	}
}

// HandleRequest responds to the request recieved.
func HandleRequest(server *Server, request []string) error {
	if len(request) == 0 {
		return fmt.Errorf("empty request")
	}

	switch request[0] {
	case "PING":
		err := handlePing(server.c)
		if err != nil {
			return fmt.Errorf("PING failed: %v", err)
		}
	case "ECHO":
		if len(request) != 2 {
			return fmt.Errorf("ECHO expects 1 argument")
		}
		err := handleEcho(server.c, request[1])
		if err != nil {
			return fmt.Errorf("ECHO failed: %v", err)
		}
	case "SET":
		err := handleSet(server, request[1:])
		if err != nil {
			return fmt.Errorf("SET failed: %v", err)
		}

		fmt.Println("testing set in handlereq")

		if server.opts.Role == "master" {
			err := handlePropagation(request)
			if err != nil {
				return fmt.Errorf("Propagation failed: %v", err)
			}
		}
	case "GET":
		if len(request) != 2 {
			return fmt.Errorf("GET expects 1 argument")
		}
		err := handleGet(server, request[1])
		if err != nil {
			return fmt.Errorf("GET failed: %v", err)
		}

		fmt.Println("testing get in handlereq")

		if server.opts.Role == "master" {
			err := handlePropagation(request)
			if err != nil {
				return fmt.Errorf("Propagation failed: %v", err)
			}
		}
	case "INFO":
		if len(request) != 2 {
			return fmt.Errorf("INFO expects 1 argument")
		}
		err := handleInfo(request[1], server)
		if err != nil {
			return fmt.Errorf("INFO failed: %v", err)
		}
	case "REPLCONF":
		err := handleReplconf(server.c)
		if err != nil {
			return fmt.Errorf("REPLCONF failed: %v", err)
		}
		remoteAddr, conn := server.c.conn.RemoteAddr().String(), server.c
		Repls[remoteAddr] = conn
	case "PSYNC":
		err := handlePsync(request[1:], server)
		if err != nil {
			return fmt.Errorf("PSYNC failed: %v", err)
		}
	default:
		return fmt.Errorf("unknown command: %s", request[0])
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

func handleSet(s *Server, request []string) error {
	fmt.Println("set test 1")
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
	fmt.Println("set test 1.5")

	s.storage.cache[key] = NewEntry(value, expireAt)

	fmt.Println("set test 2")
	err := s.c.Write("+OK\r\n")
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	fmt.Println("set test 3")
	return nil
}

func handleGet(s *Server, key string) error {
	fmt.Println("get test 1")
	now := time.Now().UnixMilli()

	entry, ok := s.storage.cache[key]
	if !ok {
		err := s.c.Write("$-1\r\n")
		if err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
		return nil
	}

	fmt.Println("get test 2")

	if entry.expireAt != 0 && now > entry.expireAt {
		err := s.c.Write("$-1\r\n")
		if err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
		delete(s.storage.cache, key)
		return nil
	}

	fmt.Println("get test 3")

	err := s.c.Write(fmt.Sprintf("$%d\r\n%s\r\n", len(entry.msg), entry.msg))
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	fmt.Println("get test 4")

	return nil
}

func handleInfo(arg string, server *Server) error {
	var s string

	if arg == "replication" {
		s += "# Replication\r\n"
		if server.opts.Role == "slave" {
			s += "role:slave\r\n"
		} else {
			s += "role:master\r\n"
		}

		s += fmt.Sprintf("master_replid:%s\r\n", server.opts.ReplID)

		s += fmt.Sprintf("master_repl_offset:%d\r\n", server.opts.ReplOffset)
	}

	err := server.c.Write(fmt.Sprintf("$%d\r\n%s\r\n", len(s), s))
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
		err2 := server.c.Write(fmt.Sprintf("+FULLRESYNC %s 0\r\n", server.opts.ReplID))
		if err2 != nil {
			return fmt.Errorf("Write failed: %v", err2)
		}
	}

	err := server.c.Write(fmt.Sprintf("$%d\r\n%s", len(string(emptyRDB)), string(emptyRDB)))
	if err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}

func handlePropagation(command []string) error {
	propCmd := ToRespArray(command)
	fmt.Println("testing propagation " + propCmd)

	for _, conn := range Repls {
		err := conn.Write(propCmd)
		if err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
	}

	return nil
}
