package protocol

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
