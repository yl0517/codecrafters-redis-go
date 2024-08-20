package protocol

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"time"
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
func (s *Server) processRDB() error {
	path := fmt.Sprintf("%s/%s", s.opts.Dir, s.opts.Dbfilename)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("os.Open failed: %v", err)
	}
	defer f.Close()
	file := NewFile(f)

	err = s.addKVPair(file)
	if err != nil {
		return fmt.Errorf("addKVPair failed: %v", err)
	}

	return nil
}

// addKVPair parses key-value pairs from the RDB file
func (s *Server) addKVPair(file *File) error {
	dbSelected := false
	for !dbSelected {
		b, err := file.reader.ReadByte()
		if err != nil {
			return fmt.Errorf("ReadByte failed: %v", err)
		}

		if b == opSelectDB {
			dbSelected = true
			lengthByte, err := file.reader.ReadByte()
			if err != nil {
				return fmt.Errorf("ReadByte failed for DB index length: %v", err)
			}

			dbIndex, err := file.parseLength(lengthByte)
			if err != nil {
				return fmt.Errorf("parseLength failed for DB index: %v", err)
			}

			fmt.Printf("Selected DB: %d\n", dbIndex)
		}
	}

	var expiry int64 = 0
	for {
		b, err := file.reader.ReadByte()
		if err != nil {
			return fmt.Errorf("ReadByte failed: %v", err)
		}

		switch b {
		case opExpireTime:
			expiry, err = file.readExpireTime()
			if err != nil {
				return fmt.Errorf("readExpireTime failed: %v", err)
			}
			fmt.Println("Encountered opExpireTime")

		case opExpireTimeMS:
			expiry, err = file.readExpireTimeMS()
			if err != nil {
				return fmt.Errorf("readExpireTimeMS failed: %v", err)
			}
			fmt.Println("Encountered opExpireTimeMS")

		case opResizeDB:
			fmt.Println("Encountered opResizeDB")
			b, err = file.reader.ReadByte()
			if err != nil {
				return fmt.Errorf("ReadByte failed: %v", err)
			}

			dbHashTableSize, err := file.parseLength(b)
			if err != nil {
				return fmt.Errorf("parseLength failed for dbHashTableSize: %v", err)
			}
			fmt.Printf("dbHashTableSize: %d\n", dbHashTableSize)

			b, err = file.reader.ReadByte()
			if err != nil {
				return fmt.Errorf("ReadByte failed: %v", err)
			}

			expireHashTableSize, err := file.parseLength(b)
			if err != nil {
				return fmt.Errorf("parseLength failed for expireHashTableSize: %v", err)
			}
			fmt.Printf("expireHashTableSize: %d\n", expireHashTableSize)

		case opAux:
			fmt.Println("Encountered opAux")
			continue

		case opEOF:
			fmt.Println("Encountered opEOF")
			return nil

		default:
			fmt.Printf("Parsing key-value pair, starting with byte: %08b\n", b)

			key, err := file.parseString(b)
			if err != nil {
				return fmt.Errorf("file.parseString failed for key: %v", err)
			}

			if key == "" {
				fmt.Println("Encountered an empty key, skipping")
				continue
			}

			fmt.Printf("Parsed key: %s\n", key)

			b, err = file.reader.ReadByte()
			if err != nil {
				return fmt.Errorf("ReadByte failed: %v", err)
			}

			value, err := file.parseString(b)
			if err != nil {
				return fmt.Errorf("file.parseString failed for value: %v", err)
			}

			// Check if the key is expired
			if expiry > 0 && expiry < time.Now().Unix()*1000 {
				fmt.Printf("Key %s has expired, skipping\n", key)
				expiry = 0
				continue
			}

			fmt.Printf("Parsed value: %s\n", value)
			fmt.Printf("Adding kv pair with expiry: %s, %s, %d\n", key, value, expiry)
			s.storage.Set(key, value, expiry)
			expiry = 0
		}
	}
}

// readExpireTime reads an expiry time in seconds
func (file *File) readExpireTime() (int64, error) {
	buf := make([]byte, 4)
	_, err := file.reader.Read(buf)
	if err != nil {
		return 0, fmt.Errorf("Read failed: %v", err)
	}
	expiry := int64(binary.LittleEndian.Uint32(buf)) * 1000 // Convert to milliseconds
	return expiry, nil
}

// readExpireTimeMS reads an expiry time in milliseconds
func (file *File) readExpireTimeMS() (int64, error) {
	buf := make([]byte, 8)
	_, err := file.reader.Read(buf)
	if err != nil {
		return 0, fmt.Errorf("Read failed: %v", err)
	}
	expiry := int64(binary.LittleEndian.Uint64(buf))
	return expiry, nil
}

// parseLength parses the length of the next object in the stream
func (file *File) parseLength(b byte) (int, error) {
	fmt.Printf("Parsing length from byte: %08b\n", b)
	msb := uint8(b >> 6)
	switch msb {
	case 0b00:
		length := int(b & 0b00111111)
		fmt.Printf("Parsed length (00): %d\n", length)
		return length, nil

	case 0b01:
		nextByte, err := file.reader.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("ReadByte failed: %v", err)
		}
		length := (int(b&0b00111111) << 8) | int(nextByte)
		fmt.Printf("Parsed length (01): %d\n", length)
		return length, nil

	case 0b10:
		next4bytes := make([]byte, 4)
		_, err := file.reader.Read(next4bytes)
		if err != nil {
			return 0, fmt.Errorf("Read failed: %v", err)
		}

		length := int(binary.BigEndian.Uint32(next4bytes))
		fmt.Printf("Parsed length (10, 32 bits): %d\n", length)
		return length, nil

	case 0b11:
		lastSixBits := uint64(b & 0b00111111)

		switch lastSixBits {
		case 0:
			l, err := file.reader.ReadByte()
			if err != nil {
				return 0, fmt.Errorf("ReadByte failed: %v", err)
			}

			length := int8(l)
			fmt.Printf("Parsed length (11): %d\n", length)
			return int(length), nil
		case 1:
			l := make([]byte, 2)
			_, err := file.reader.Read(l)
			if err != nil {
				return 0, fmt.Errorf("ReadByte failed: %v", err)
			}

			length := binary.BigEndian.Uint16(l)
			fmt.Printf("Parsed length (11): %d\n", length)
			return int(length), nil
		case 2:
			l := make([]byte, 4)
			_, err := file.reader.Read(l)
			if err != nil {
				return 0, fmt.Errorf("ReadByte failed: %v", err)
			}

			length := binary.BigEndian.Uint32(l)
			fmt.Printf("Parsed length (11): %d\n", length)
			return int(length), nil
		default:
			return 0, fmt.Errorf("invalid special encoding: %d", lastSixBits)
		}
	}

	return 0, fmt.Errorf("invalid length encoding")
}

// parseString parses a string from the RDB file
func (file *File) parseString(b byte) (string, error) {
	length, err := file.parseLength(b)
	if err != nil {
		return "", fmt.Errorf("parseLength failed: %v", err)
	}
	fmt.Printf("String length: %d\n", length)

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
