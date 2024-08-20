package protocol

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

func (s *Storage) Get(key string) (*Entry, bool) {
	entry, ok := s.cache[key]

	return entry, ok
}

func (s *Storage) Set(key string, value string, expireAt int64) {
	s.cache[key] = NewEntry(value, int64(expireAt))
}

func (s *Storage) Delete(key string) {
	delete(s.cache, key)
}

func (s *Storage) GetStream(key string) (*Stream, bool) {
	stream, ok := s.streams[key]

	return stream, ok
}

func (s *Storage) AddStream(key string) {
	s.streams[key] = NewStream()
}
