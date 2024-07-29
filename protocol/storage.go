package protocol

import (
	"fmt"
)

var storage = NewStorage()

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

type Stream struct {
	entries map[string]*StreamEntry
}

func NewStream() *Stream {
	return &Stream{
		// key - stream identifier
		// value - stream entry consisting of id and one or more kv pairs
		entries: make(map[string]*StreamEntry),
	}
}

type StreamEntry struct {
	id      string
	kvpairs map[string]string
}

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

// Storage represents the cache storage system
type Storage struct {
	cache   map[string]*Entry
	streams map[string]*Stream
}

// NewStorage is the cache storage constructor
func NewStorage() *Storage {
	return &Storage{
		cache: make(map[string]*Entry),
	}
}
