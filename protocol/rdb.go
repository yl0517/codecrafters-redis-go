package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

const (
	opAux          byte = 250
	opResizeDB     byte = 251
	opExpireTimeMS byte = 252
	opExpireTime   byte = 253
	opSelectDB     byte = 254
	opEOF          byte = 255
)

// File represents an RDB file
type File struct {
	file   *os.File
	reader *bufio.Reader
}

// NewFile creates a new File instance
func NewFile(f *os.File) *File {
	return &File{
		file:   f,
		reader: bufio.NewReader(f),
	}
}

// processRDB handles the KEYS command
func processRDB(s *Server) error {
	path := fmt.Sprintf("%s/%s", s.opts.Dir, s.opts.Dbfilename)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("os.Open failed: %v", err)
	}
	defer f.Close()
	file := NewFile(f)

	err = s.addKVPair(file)
	if err != nil {
		return fmt.Errorf("getKeys failed: %v", err)
	}

	return nil
}

// addKVPair parses key-value pairs from the RDB file
func (s *Server) addKVPair(file *File) error {
	dbSelected := false
	for !dbSelected {
		_, err := file.reader.ReadString(opSelectDB)
		if err != nil {
			return fmt.Errorf("ReadString failed: %v", err)
		}
		b, err := file.reader.ReadByte()
		if err != nil {
			return fmt.Errorf("ReadByte failed: %v", err)
		}
		file.parseString(b)

		dbSelected = true
	}

	for {
		b, err := file.reader.ReadByte()
		if err != nil {
			return fmt.Errorf("ReadByte failed: %v", err)
		}

		switch b {
		case opExpireTime:
			fmt.Println("Encountered opExpireTime")
			continue

		case opExpireTimeMS:
			fmt.Println("Encountered opExpireTimeMS")
			continue

		case opResizeDB:
			fmt.Println("Encountered opResizeDB")
			b, err = file.reader.ReadByte()
			_, err = file.parseString(b) // Database hash table size
			b, err = file.reader.ReadByte()
			_, err = file.parseString(b) // Expiry hash table size

		case opAux:
			fmt.Println("Encountered opAux")
			continue

		case opEOF:
			fmt.Println("Encountered opEOF")
			return nil

		default:
			key, err := file.parseString(b)
			if err != nil {
				return fmt.Errorf("file.parseString failed for key: %v", err)
			}

			b, err = file.reader.ReadByte()
			if err != nil {
				return fmt.Errorf("ReadByte failed: %v", err)
			}

			value, err := file.parseString(b)
			if err != nil {
				return fmt.Errorf("file.parseString failed for value: %v", err)
			}

			fmt.Printf("Adding kv pair: %s, %s\n", key, value)
			s.storage.cache[key] = NewEntry(value, 0)
		}
	}
}

// parseLength parses the length of the next object in the stream
func (file *File) parseLength(b byte) (int, error) {
	fmt.Printf("Parsing length from byte: %08b\n", b) // Debug line
	msb := uint8(b >> 6)
	switch msb {
	case 0b00:
		length := int(b & 0b00111111)
		fmt.Printf("Parsed length (00): %d\n", length) // Debug line
		return length, nil

	case 0b01:
		nextByte, err := file.reader.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("ReadByte failed: %v", err)
		}
		length := (int(b&0b00111111) << 8) | int(nextByte)
		fmt.Printf("Parsed length (01): %d\n", length) // Debug line
		return length, nil

	case 0b10:
		next4bytes := make([]byte, 4)
		_, err := file.reader.Read(next4bytes)
		if err != nil {
			return 0, fmt.Errorf("Read failed: %v", err)
		}

		lastSixBits := b & 0b00111111
		if lastSixBits == 1 {
			return int(binary.BigEndian.Uint64(next4bytes)), nil
		} else if lastSixBits == 0 {
			length := int(binary.BigEndian.Uint32(next4bytes))
			fmt.Printf("Parsed length (10, 32 bits): %d\n", length) // Debug line
			return length, nil
		}

	case 0b11:
		return 0, fmt.Errorf("special format not implemented")
	}

	return 0, fmt.Errorf("invalid length encoding")
}

// parseString parses a string from the RDB file
func (file *File) parseString(b byte) (string, error) {
	length, err := file.parseLength(b)
	if err != nil {
		return "", fmt.Errorf("parseLength failed: %v", err)
	}
	fmt.Printf("String length: %d\n", length) // Debug line

	if length < 0 {
		return "", fmt.Errorf("invalid string length: %d", length)
	}

	// Handle empty string case
	if length == 0 {
		return "", nil
	}

	str := make([]byte, length)
	n, err := file.reader.Read(str)
	if err != nil {
		return "", fmt.Errorf("Read failed: %v", err)
	}
	if n != length {
		return "", fmt.Errorf("read string length mismatch: expected %d, got %d", length, n)
	}
	fmt.Printf("Parsed string: %s\n", string(str[:n]))
	return string(str[:n]), nil
}
