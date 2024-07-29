package protocol

import (
	"fmt"
	"net"
	"sync"
)

var repls = NewSlaves()

// Slaves store secondary connections
type Slaves struct {
	list map[string]*Connection
	lock sync.RWMutex
}

// NewSlaves is the Repls constructor
func NewSlaves() *Slaves {
	return &Slaves{
		list: make(map[string]*Connection),
		lock: sync.RWMutex{},
	}
}

// AddSlave adds a new slave to the internal map.
func (s *Slaves) AddSlave(slaveAddr net.Addr, conn *Connection) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.list[slaveAddr.String()] = conn
}

func (s *Slaves) Ack(slaveAddr net.Addr, ack int) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	slave, ok := s.list[slaveAddr.String()]
	if !ok {
		return fmt.Errorf("couldn't find slave: %s", slaveAddr.String())
	}

	slave.offset = ack
	return nil
}

func (s *Slaves) Propagate(cmd string) error {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, s := range s.list {
		if err := s.Write(cmd); err != nil {
			return fmt.Errorf("propagation write failed: %w", err)
		}
	}
	return nil
}

// SyncedSlaveCount returns the number of slaves that are synced with the given master status.
func (s *Slaves) SyncedSlaveCount(masterOffset int) int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	ret := 0

	for _, s := range s.list {
		fmt.Println("s.offset = ", s.offset)
		if s.offset == masterOffset {
			ret++
		}
	}

	return ret
}

func (s *Slaves) Count() int {
	s.lock.RLock()
	defer s.lock.RUnlock()

	return len(s.list)
}
