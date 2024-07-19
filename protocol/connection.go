package protocol

import (
	"bufio"
	"fmt"
	"net"
)

// Connection represents a connection between a client and a server.
type Connection struct {
	conn   net.Conn
	reader *bufio.Reader
	offset int
}

// NewConnection creates a new Connection instance.
func NewConnection(c net.Conn) *Connection {
	return &Connection{
		conn:   c,
		reader: bufio.NewReader(c),
		offset: 0,
	}
}

// Close closes the connection.
func (c *Connection) Close() error {
	return c.conn.Close()
}

// GetLine returns an individual line from a command without CRLF
func (c *Connection) GetLine() (int, string, error) {
	s, err := c.reader.ReadString('\n')

	numBytes := len(s)

	if len(s) > 0 {
		if s[len(s)-1] == '\n' {
			s = s[:len(s)-1]
		}

		if s[len(s)-1] == '\r' {
			s = s[:len(s)-1]
		}
	}

	return numBytes, s, err
}

// Write writes the given string to the connection
func (c *Connection) Write(s string) error {
	var written int

	for written < len(s) {
		n, err := c.conn.Write([]byte(s[written:]))
		if err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
		written += n
	}
	return nil
}

// Read takes a RESP array and returns the individual requests inside a slice and the offset
func (s *Server) Read() (int, []string, error) {
	var o int

	// defer func() {
	// 	s.offset += o
	// }()

	bytes, numElem, err := s.c.GetLine()
	if err != nil {
		return 0, nil, fmt.Errorf("c.GetLine() failed: %w", err)
	}
	o += bytes

	len, err := GetArrayLength(numElem)
	if err != nil {
		return 0, nil, fmt.Errorf("GetArrayLength() failed: %w", err)
	}

	var request []string

	for i := 0; i < len; i++ {
		bytes, line, err := s.c.GetLine()
		if err != nil {
			return 0, nil, fmt.Errorf("c.GetLine() failed: %w", err)
		}
		o += bytes

		len, err := GetBulkStringLength(line)
		if err != nil {
			return 0, nil, fmt.Errorf("GetBulkStringLength() failed: %w", err)
		}

		bytes, s, err := s.c.GetLine()
		if err != nil {
			return 0, nil, fmt.Errorf("c.GetLine() failed: %w", err)
		}
		o += bytes

		err = VerifyBulkStringLength(s, len)
		if err != nil {
			return 0, nil, fmt.Errorf("VerifyBulkStringLength() failed: %w", err)
		}

		request = append(request, s)
	}

	return o, request, nil
}
