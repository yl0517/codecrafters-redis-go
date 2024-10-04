package protocol

import "time"

var storage = NewStorage()

// Entry represents the cache entry.
type Entry struct {
	value    string
	expireAt int64
}

// NewEntry is the Entry constructor.
func NewEntry(s string, t int64) *Entry {
	return &Entry{
		value:    s,
		expireAt: t,
	}
}

// Storage represents the cache storage system
type Storage struct {
	cache   map[string]*Entry
	streams map[string]*Stream // stream key, stream
}

// NewStorage is the cache storage constructor
func NewStorage() *Storage {
	return &Storage{
		cache:   make(map[string]*Entry),
		streams: make(map[string]*Stream),
	}
}

// Get returns the string value mapped to the given key.
// nil will be returned if the entry expired or there's no such item.
func (s *Storage) Get(key string) *string {
	entry, ok := s.cache[key]
	if !ok {
		return nil
	}

	if entry.expireAt != 0 && time.Now().UnixMilli() > entry.expireAt {
		s.Delete(key)
		return nil
	}

	return &entry.value
}

// Set adds a new entry to the storage
func (s *Storage) Set(key string, value string, expireAt int64) {
	s.cache[key] = NewEntry(value, int64(expireAt))
}

// Delete removes a cache entry with the given key
func (s *Storage) Delete(key string) {
	delete(s.cache, key)
}

// GetStream returns the Stream mapped to the given key
func (s *Storage) GetStream(key string) (*Stream, bool) {
	stream, ok := s.streams[key]

	return stream, ok
}

// AddStream adds a new stream to the storage
func (s *Storage) AddStream(key string) {
	s.streams[key] = NewStream()
}
