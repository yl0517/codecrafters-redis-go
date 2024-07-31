package protocol

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Stream represents a stream
type Stream struct {
	entries []*StreamEntry
}

// NewStream is the Stream constructor
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

// NewStreamEntry is the StreamEntry constructor
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

func validateStreamEntryID(stream *Stream, id string) (string, error) {
	millisecondsTime, err := strconv.Atoi(id[:strings.IndexByte(id, '-')])
	if err != nil {
		return "", fmt.Errorf("Atoi failed: %v", err)
	}
	sequenceNumber, err := strconv.Atoi(id[strings.IndexByte(id, '-')+1:])
	if err != nil {
		return "", fmt.Errorf("Atoi failed: %v", err)
	}

	if len(stream.entries) == 0 {
		if millisecondsTime > 0 || sequenceNumber > 0 {
			return "", nil
		}
	}

	if millisecondsTime == 0 && sequenceNumber == 0 {
		return "ERR The ID specified in XADD must be greater than 0-0", nil
	}

	prevID := stream.entries[len(stream.entries)-1].id

	prevMillisecondsTime, err := strconv.Atoi(prevID[:strings.IndexByte(prevID, '-')])
	if err != nil {
		return "", fmt.Errorf("Atoi failed: %v", err)
	}
	prevSequenceNumber, err := strconv.Atoi(prevID[strings.IndexByte(prevID, '-')+1:])
	if err != nil {
		return "", fmt.Errorf("Atoi failed: %v", err)
	}

	fmt.Println(millisecondsTime, prevMillisecondsTime, sequenceNumber, prevSequenceNumber)

	if millisecondsTime > prevMillisecondsTime {
		return "", nil
	} else if millisecondsTime == prevMillisecondsTime {
		if sequenceNumber > prevSequenceNumber {
			return "", nil
		}

	}
	return "ERR The ID specified in XADD is equal or smaller than the target stream top item", nil

}

func autoGenSeqNum(stream *Stream, id string) (string, error) {
	millisecondsTime, err := strconv.Atoi(id[:strings.IndexByte(id, '-')])
	if err != nil {
		return "", fmt.Errorf("Atoi failed: %v", err)
	}

	if len(stream.entries) == 0 {
		if millisecondsTime == 0 {
			// default seq num is 1 when time is 0
			return "1", nil
		}

		return "0", nil
	}
	prevID := stream.entries[len(stream.entries)-1].id

	prevMillisecondsTime, err := strconv.Atoi(prevID[:strings.IndexByte(prevID, '-')])
	if err != nil {
		return "", fmt.Errorf("Atoi failed: %v", err)
	}
	prevSequenceNumber, err := strconv.Atoi(prevID[strings.IndexByte(prevID, '-')+1:])
	if err != nil {
		return "", fmt.Errorf("Atoi failed: %v", err)
	}

	if millisecondsTime > prevMillisecondsTime {
		if millisecondsTime == 0 {
			// default seq num is 1 when time is 0
			return "1", nil
		}

		return "0", nil
	}

	fmt.Println("we done")

	return fmt.Sprintf("%d", prevSequenceNumber+1), nil
}

func autoGenID(stream *Stream) (string, error) {
	millisecondsTime := time.Now().UnixMilli()
	sequenceNumber := 0

	if len(stream.entries) > 0 {
		prevID := stream.entries[len(stream.entries)-1].id

		prevMillisecondsTime, err := strconv.Atoi(prevID[:strings.IndexByte(prevID, '-')])
		if err != nil {
			return "", fmt.Errorf("Atoi failed: %v", err)
		}
		prevSequenceNumber, err := strconv.Atoi(prevID[strings.IndexByte(prevID, '-')+1:])
		if err != nil {
			return "", fmt.Errorf("Atoi failed: %v", err)
		}

		if millisecondsTime == int64(prevMillisecondsTime) {
			sequenceNumber = prevSequenceNumber + 1
		}
	}

	return fmt.Sprintf("%d-%d", millisecondsTime, sequenceNumber), nil
}
