package protocol

import "fmt"

// Stream represents a stream
type Stream struct {
	entries []*StreamEntry
}

// NewStream creates a slice of stream entries (append only)
func NewStream() *Stream {
	return &Stream{
		entries: make([]*StreamEntry, 0),
	}
}

// StreamEntry represents each individual entry added to a stream
type StreamEntry struct {
	id      string
	kvpairs map[string]string
}

// NewStreamEntry creates a new stream entry
func NewStreamEntry(id string, kvs []string) (*StreamEntry, error) {
	if len(kvs)%2 != 0 {
		return nil, fmt.Errorf("invalid number of keys or values: %v", kvs)
	}

	kvpairs := make(map[string]string)

	for i := 0; i < len(kvs); i += 2 {
		kvpairs[kvs[i]] = kvs[i+1]
	}

	return &StreamEntry{
		id:      id,
		kvpairs: kvpairs,
	}, nil
}
