package protocol

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Server represents a server
type Server struct {
	c       *Connection
	opts    Opts
	storage *Storage
	queuing bool
	queue   [][]string

	// for master only
	mc *MasterConfig
}

// NewMaster is the master constructor
func NewMaster(conn *Connection, o Opts, mc *MasterConfig) *Server {
	return &Server{
		c:       conn,
		opts:    o,
		storage: storage,
		mc:      mc,
		queuing: false,
		queue:   make([][]string, 0),
	}
}

// MasterConfig represents the configuration used by master
type MasterConfig struct {
	slaves     *Slaves
	wg         *sync.WaitGroup
	propOffset int
}

// NewMasterConfig is the MasterConfig constructor
func NewMasterConfig() *MasterConfig {
	return &MasterConfig{
		slaves:     list,
		propOffset: 0,
	}
}

// NewSlave is the slave constructor
func NewSlave(conn *Connection) *Server {
	return &Server{
		c:       conn,
		storage: storage,
	}
}

// Handle reads and handles
func (s *Server) Handle() {
	defer s.c.Close()

	s.processRDB()

	for {
		o, request, err := s.Read()
		if err != nil {
			fmt.Printf("conn.Read() failed: %v\n", err)
			return
		}

		fmt.Printf("Received request: %v\n", request)

		err = s.HandleRequest(request)
		if err != nil {
			fmt.Printf("protocol.HandleRequest() failed: %v\n", err)
		}

		if s.opts.Role != "master" && (len(request) <= 1 || request[1] != "GETACK") {
			s.c.offset += o
		}
	}
}

// HandleRequest responds to the request recieved.
func (s *Server) HandleRequest(request []string) error {
	if len(request) == 0 {
		return fmt.Errorf("empty request")
	}

	switch strings.ToUpper(request[0]) {
	case "EXEC":
		if err := handleExec(s); err != nil {
			return fmt.Errorf("MULTI failed: %v", err)
		}
	case "MULTI":
		if err := handleMulti(s); err != nil {
			return fmt.Errorf("MULTI failed: %v", err)
		}
	case "DISCARD":
		if err := handleDiscard(s); err != nil {
			return fmt.Errorf("MULTI failed: %v", err)
		}
	default:
		if s.queuing {
			s.queue = append(s.queue, request)

			if err := s.c.Write("+QUEUED\r\n"); err != nil {
				return fmt.Errorf("Write failed: %v", err)
			}

			return nil
		}

		response, err := s.processRequest(request)
		if err != nil {
			return fmt.Errorf("processRequest failed: %v", err)
		}

		s.c.Write(response)
	}

	return nil
}

func handleMulti(s *Server) error {
	if !s.queuing {
		s.queuing = true
	} else {
		return errors.New("Multi Already Called")
	}

	if err := s.c.Write("+OK\r\n"); err != nil {
		return fmt.Errorf("Write failed: %v", err)
	}

	return nil
}

func handleExec(s *Server) error {
	if !s.queuing {
		if err := s.c.Write("-ERR EXEC without MULTI\r\n"); err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
	} else {
		if len(s.queue) == 0 {
			if s.queuing != false {
				s.queuing = false
			}

			if err := s.c.Write("*0\r\n"); err != nil {
				return fmt.Errorf("Write failed: %v", err)
			}
		} else {
			s.queuing = false
			responses := []string{}

			for _, request := range s.queue {
				response, err := s.processRequest(request)
				if err != nil {
					return fmt.Errorf("processRequest failed: %v", err)
				}

				if response == "" {
					continue
				}

				responses = append(responses, response)
			}

			s.queue = [][]string{}

			respArr := fmt.Sprintf("*%d\r\n", len(responses))
			for _, s := range responses {
				respArr += s
			}

			s.c.Write(respArr)
		}
	}

	return nil
}

func handleDiscard(s *Server) error {
	if s.queuing {
		s.queuing = false
		s.queue = [][]string{}

		if err := s.c.Write("+OK\r\n"); err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
	} else {
		if err := s.c.Write("-ERR DISCARD without MULTI\r\n"); err != nil {
			return fmt.Errorf("Write failed: %v", err)
		}
	}

	return nil
}

var (
	waitLock = sync.Mutex{}
)

func (s *Server) processRequest(request []string) (string, error) {
	var response string
	var err error

	switch strings.ToUpper(request[0]) {
	case "PING":
		response, err = handlePing(s)
		if err != nil {
			return "", fmt.Errorf("PING failed: %v", err)
		}
	case "ECHO":
		if len(request) != 2 {
			return "", fmt.Errorf("ECHO expects 1 argument")
		}
		response = handleEcho(request[1])
	case "SET":
		response, err = handleSet(s, request[1:])
		if err != nil {
			return "", fmt.Errorf("SET failed: %v", err)
		}

		if s.opts.Role == "master" {
			err := handlePropagation(s, request)
			if err != nil {
				return "", fmt.Errorf("Propagation failed: %v", err)
			}
		}
	case "GET":
		if len(request) != 2 {
			return "", fmt.Errorf("GET expects 1 argument")
		}
		response, err = handleGet(s, request[1])
		if err != nil {
			return "", fmt.Errorf("GET failed: %v", err)
		}
	case "INFO":
		if len(request) != 2 {
			return "", fmt.Errorf("INFO expects 1 argument")
		}
		response, err = handleInfo(request[1], s)
		if err != nil {
			return "", fmt.Errorf("INFO failed: %v", err)
		}
	case "REPLCONF":
		response, err = handleReplconf(s, request[1:])
		if err != nil {
			return "", fmt.Errorf("REPLCONF failed: %v", err)
		}
	case "PSYNC":
		response, err = handlePsync(request[1:], s)
		if err != nil {
			return "", fmt.Errorf("PSYNC failed: %v", err)
		}

		s.mc.slaves.AddSlave(s.c.conn.RemoteAddr(), s.c)
	case "WAIT":
		waitLock.Lock()
		response, err = handleWait(request[1:], s)
		waitLock.Unlock()
		if err != nil {
			return "", fmt.Errorf("WAIT failed: %v", err)
		}
	case "CONFIG":
		if request[1] == "GET" {
			response, err = handleConfigGet(request[2:], s)
			if err != nil {
				return "", fmt.Errorf("CONFIG GET failed: %v", err)
			}
		} else {
			return "", fmt.Errorf("Invalid CONFIG command: %s", request[1])
		}
	case "KEYS":
		if request[1] == "*" {
			response, err = handleKeys(s)
			if err != nil {
				return "", fmt.Errorf("KEYS failed: %v", err)
			}
		}
	case "TYPE":
		response, err = handleType(request[1:], s)
		if err != nil {
			return "", fmt.Errorf("TYPE failed: %v", err)
		}
	case "XADD":
		response, err = handleXadd(request[1:], s)
		if err != nil {
			return "", fmt.Errorf("XADD failed: %v", err)
		}
	case "XRANGE":
		response, err = handleXrange(request[1:], s)
		if err != nil {
			return "", fmt.Errorf("XRANGE failed: %v", err)
		}
	case "XREAD":
		if request[1] == "streams" {
			response, err = handleXread(-1, request[2:], s)
			if err != nil {
				return "", fmt.Errorf("XREAD failed: %v", err)
			}
		} else if request[1] == "block" {
			timeout, err := strconv.Atoi(request[2])
			if err != nil {
				return "", fmt.Errorf("Atoi failed: %v", err)
			}

			if request[3] != "streams" {
				return "", fmt.Errorf("XREAD block %d be followed by \"streams\", found: %s", timeout, request[1])
			}

			response, err = handleXread(timeout, request[4:], s)
			if err != nil {
				return "", fmt.Errorf("XREAD failed: %v", err)
			}
		} else {
			return "", fmt.Errorf("XREAD must be followed by \"streams\", found: %s", request[1])
		}
	case "INCR":
		response, err = handleIncr(request[1], s)
		if err != nil {
			return "", fmt.Errorf("INCR failed: %v", err)
		}
	default:
		return "", fmt.Errorf("unknown command: %s", request[0])
	}

	return response, nil
}

func handlePing(s *Server) (string, error) {
	if s.opts.Role == "master" {
		return "+PONG\r\n", nil
	}

	return "", fmt.Errorf("temp: %s", "temp")
}

func handleEcho(message string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(message), message)
}

func handleSet(s *Server, request []string) (string, error) {
	key := request[0]
	value := request[1]

	var expireAt int64
	if len(request) == 4 {
		expireAfter, err := strconv.ParseInt(request[3], 10, 64)
		if err != nil {
			return "", fmt.Errorf("Atoi failed: %v", err)
		}
		expireAt = time.Now().UnixMilli() + expireAfter
	}

	s.storage.cache[key] = NewEntry(value, expireAt)

	if s.opts.Role == "master" {
		return "+OK\r\n", nil
	}

	return "", nil
}

func handleGet(s *Server, key string) (string, error) {
	now := time.Now().UnixMilli()

	entry, ok := s.storage.cache[key]
	if !ok {
		return "$-1\r\n", nil
	}

	if entry.expireAt != 0 && now > entry.expireAt {
		delete(s.storage.cache, key)
		return "$-1\r\n", nil
	}

	return fmt.Sprintf("$%d\r\n%s\r\n", len(entry.msg), entry.msg), nil
}

func handleInfo(arg string, s *Server) (string, error) {
	var ret string

	if arg == "replication" {
		ret += "# Replication\r\n"
		if s.opts.Role == "slave" {
			ret += "role:slave\r\n"
		} else {
			ret += "role:master\r\n"
		}

		ret += fmt.Sprintf("master_replid:%s\r\n", s.opts.ReplID)

		ret += fmt.Sprintf("master_repl_offset:%d\r\n", s.mc.propOffset)
	} else {
		return "", fmt.Errorf("temp: %s", "temp")
	}

	return fmt.Sprintf("$%d\r\n%s\r\n", len(ret), ret), nil
}

func handleReplconf(s *Server, request []string) (string, error) {
	switch request[0] {
	case "ACK":
		// This logic is ran by master
		ack, err := strconv.Atoi(request[1])
		if err != nil {
			return "", fmt.Errorf("strconf.Atoi failed: %v", err)
		}

		fmt.Println("ack = ", ack)

		if err := s.mc.slaves.Ack(s.c.conn.RemoteAddr(), ack); err != nil {
			return "", fmt.Errorf("ack slave response filed: %w", err)
		}

		if s.mc.wg != nil {
			s.mc.wg.Done()
		}
	case "GETACK":
		// This logic is ran by slave
		curr := s.c.offset
		ret := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$%d\r\n%d\r\n", len(strconv.Itoa(curr)), curr)
		s.c.offset += 37

		return ret, nil
	default:
		return "+OK\r\n", nil
	}

	return "", nil
}

func handlePsync(request []string, server *Server) (string, error) {
	var ret string

	emptyRDB, err := base64.StdEncoding.DecodeString("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
	if err != nil {
		return "", fmt.Errorf("DecodeString failed: %v", err)
	}

	if request[0] == "?" {
		ret += fmt.Sprintf("+FULLRESYNC %s 0\r\n", server.opts.ReplID)

	}

	ret += fmt.Sprintf("$%d\r\n%s", len(string(emptyRDB)), string(emptyRDB))

	server.mc.slaves.AddSlave(server.c.conn.RemoteAddr(), server.c)

	return ret, nil
}

func handlePropagation(master *Server, request []string) error {
	propCmd := ToRespArray(request)
	if err := master.mc.slaves.Propagate(propCmd); err != nil {
		return fmt.Errorf("cannot propagate: %w", err)
	}
	master.mc.propOffset += len(propCmd)

	return nil
}

func handleWait(request []string, master *Server) (string, error) {
	numReplicas, err := strconv.Atoi(request[0])
	if err != nil {
		return "", fmt.Errorf("Atoi failed: %v", err)
	}

	t, err := strconv.Atoi(request[1])
	if err != nil {
		return "", fmt.Errorf("Atoi failed: %v", err)
	}

	notAcked := master.mc.slaves.NotSyncedSlaveCount(master.mc.propOffset)

	if notAcked == 0 {
		return fmt.Sprintf(":%d\r\n", master.mc.slaves.Count()), nil

	}

	if err := master.mc.slaves.Propagate("*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n"); err != nil {
		return "", fmt.Errorf("Propagate failed: %v", err)
	}

	master.mc.wg = &sync.WaitGroup{}
	master.mc.wg.Add(min(numReplicas, master.mc.slaves.Count()))

	ch := make(chan struct{})
	go func() {
		defer close(ch)
		master.mc.wg.Wait()
	}()

	select {
	case <-ch:
	case <-time.After(time.Duration(t) * time.Millisecond):
	}

	notAcked = master.mc.slaves.NotSyncedSlaveCount(master.mc.propOffset)

	fmt.Printf("master.offset = %d, not acked = %d\n", master.mc.propOffset, notAcked)

	master.mc.propOffset += 37

	return fmt.Sprintf(":%d\r\n", master.mc.slaves.Count()-notAcked), nil
}

func handleConfigGet(request []string, s *Server) (string, error) {
	switch request[0] {
	case "dir":
		return fmt.Sprintf("*2\r\n$3\r\ndir\r\n$%d\r\n%s\r\n", len(s.opts.Dir), s.opts.Dir), nil
	case "dbfilename":
		return fmt.Sprintf("*2\r\n$3\r\ndbfilename\r\n$%d\r\n%s\r\n", len(s.opts.Dbfilename), s.opts.Dbfilename), nil
	default:
		return "", fmt.Errorf("Invalid config get param: %v", request[0])
	}
}

func handleKeys(s *Server) (string, error) {
	var keys []string
	for k := range s.storage.cache {
		fmt.Printf("Found key: %s\n", k)
		keys = append(keys, k)
	}

	return ToRespArray(keys), nil
}

func handleType(request []string, s *Server) (string, error) {
	if len(request) > 1 {
		return "", fmt.Errorf("Invalid key in type command: %s", request)
	}

	fmt.Println(request[0])

	_, ok := s.storage.streams[request[0]]
	if ok {
		return "+stream\r\n", nil

	}

	_, ok = s.storage.cache[request[0]]
	if ok {
		return "+string\r\n", nil
	}

	return "+none\r\n", nil
}

func handleXadd(request []string, s *Server) (string, error) {
	_, ok := s.storage.streams[request[0]]
	if !ok {
		s.storage.streams[request[0]] = NewStream()
	}
	stream := s.storage.streams[request[0]]

	id := request[1]
	if strings.Contains(id, "*") {
		if id == "*" {
			genID, err := autoGenID(stream)
			if err != nil {
				return "", fmt.Errorf("AutoGenID failed: %v", err)
			}
			id = genID
		} else {
			if !strings.Contains(id[strings.IndexByte(id, '-')+1:], "*") {
				return "", fmt.Errorf("Invalid auto generated id request: %s", id)
			}

			// Auto Generate Seq
			seq, err := autoGenSeqNum(stream, id)
			if err != nil {
				return "", fmt.Errorf("autoGenSeqNum failed: %v", err)
			}

			id = fmt.Sprintf("%s-%s", id[:strings.IndexByte(id, '-')], seq)
		}
	}

	msg, err := validateStreamEntryID(stream, id)
	if err != nil {
		return "", err
	}
	if msg != "" {
		return msg, nil
	}

	entry, err := NewStreamEntry(id, request[2:])
	if err != nil {
		return "", fmt.Errorf("NewStreamEntry failed: %v", err)
	}

	stream.entries = append(stream.entries, entry)

	if s.mc.wg != nil {
		s.mc.wg.Done()
	}

	return ToBulkString(id), nil
}

func handleXrange(request []string, s *Server) (string, error) {
	if len(request) != 3 {
		return "", fmt.Errorf("invalid XRANGE request: %s", request)
	}

	key := request[0]

	var startIdx int
	foundStart := false
	var endIdx int
	foundEnd := false

	stream := s.storage.streams[key].entries

	startMilli, startSeq := 0, 0

	if request[1] == "-" {
		startIdx = 0
		foundStart = true
	} else if strings.Contains(request[1], "-") {
		milli, seq, err := getTimeAndSeq(request[1])
		if err != nil {
			return "", fmt.Errorf("getTimeSeq failed: %v", err)
		}

		startMilli = milli
		startSeq = seq
	} else {
		milli, err := strconv.Atoi(request[1])
		if err != nil {
			return "", fmt.Errorf("Atoi failed: %v", err)
		}

		startMilli = milli
	}

	endMilli := 0
	endSeq := 0

	if request[2] == "+" {
		endIdx = len(stream)
		foundEnd = true
	} else if strings.Contains(request[2], "-") {
		milli, seq, err := getTimeAndSeq(request[2])
		if err != nil {
			return "", fmt.Errorf("getTimeSeq failed: %v", err)
		}

		endMilli = milli
		endSeq = seq
	} else {
		milli, err := strconv.Atoi(request[2])
		if err != nil {
			return "", fmt.Errorf("Atoi failed: %v", err)
		}

		endMilli = milli
		endSeq = -1
	}

	if !foundStart || !foundEnd {
		if endSeq < 0 {
			for i, entry := range stream {
				milli, seq, err := getTimeAndSeq(entry.id)
				if err != nil {
					return "", fmt.Errorf("getTimeSeq failed: %v", err)
				}

				if !foundStart {
					if milli == startMilli && seq == startSeq {
						startIdx = i
						foundStart = true

						if foundStart && foundEnd {
							break
						}
					}
				}

				if !foundEnd {
					if milli > endMilli {
						endIdx = i
						foundEnd = true
						break
					}
				}
			}
		} else {
			for i, entry := range stream {
				milli, seq, err := getTimeAndSeq(entry.id)
				if err != nil {
					return "", fmt.Errorf("getTimeSeq failed: %v", err)
				}

				if !foundStart {
					if milli == startMilli && seq == startSeq {
						startIdx = i
						foundStart = true

						if foundStart && foundEnd {
							break
						}
					}
				}

				if !foundEnd {
					if milli == endMilli && seq == endSeq {
						endIdx = i + 1
						foundEnd = true
						break
					}
				}
			}
		}
	}

	entries := stream[startIdx:endIdx]

	resp := fmt.Sprintf("*%d\r\n", len(entries))
	for _, entry := range entries {
		resp += fmt.Sprintf("*2\r\n")
		resp += ToBulkString(entry.id)
		resp += fmt.Sprintf("*%d\r\n", len(entry.kvpairs)*2)
		for k, v := range entry.kvpairs {
			resp += ToBulkString(k)
			resp += ToBulkString(v)
		}
	}

	return resp, nil
}

func handleXread(timeout int, request []string, s *Server) (string, error) {
	streams := s.storage.streams
	curr := make([]*StreamEntry, len(s.storage.streams[request[0]].entries))
	copy(curr, s.storage.streams[request[0]].entries)

	if timeout > 0 {
		time.Sleep(time.Duration(timeout) * time.Millisecond)
	} else if timeout == 0 {
		s.mc.wg = &sync.WaitGroup{}
		s.mc.wg.Add(1)

		ch := make(chan struct{})
		go func() {
			defer close(ch)
			s.mc.wg.Wait()
		}()

		select {
		case <-ch:
		}
	}

	if len(request) < 2 || len(request)%2 != 0 {
		return "", fmt.Errorf("invalid request for XREAD: %v", request)
	}

	responses := make([]string, 0)

	for i := 0; i < (len(request))/2; i++ {
		streamKey := request[i]
		streamID := request[(len(request))/2+i]

		if streamID == "$" {
			if len(curr) == len(streams[streamKey].entries) {
				return "$-1\r\n", nil
			}
			streamID = streams[streamKey].entries[len(streams[streamKey].entries)-1-(len(streams[streamKey].entries)-len(curr))].id
		}

		reqMilli, reqSeq, err := getTimeAndSeq(streamID)
		if err != nil {
			return "", fmt.Errorf("getTimeAndSeq failed: %v", err)
		}

		stream, exists := streams[streamKey]
		if !exists {
			continue
		}

		startIdx := -1
		for j, entry := range stream.entries {
			milli, seq, err := getTimeAndSeq(entry.id)
			if err != nil {
				return "", fmt.Errorf("getTimeAndSeq failed: %v", err)
			}

			if milli > reqMilli || (milli == reqMilli && seq > reqSeq) {
				startIdx = j
				break
			}
		}

		if startIdx == -1 {
			continue
		}

		entries := stream.entries[startIdx:]
		if len(entries) == 0 {
			continue
		}

		resp := fmt.Sprintf("*2\r\n")
		resp += ToBulkString(streamKey)
		resp += fmt.Sprintf("*%d\r\n", len(entries))
		for _, entry := range entries {
			resp += fmt.Sprintf("*2\r\n")
			resp += ToBulkString(entry.id)
			resp += fmt.Sprintf("*%d\r\n", len(entry.kvpairs)*2)
			for k, v := range entry.kvpairs {
				resp += ToBulkString(k)
				resp += ToBulkString(v)
			}
		}

		responses = append(responses, resp)
	}

	if len(responses) == 0 {
		return "$-1\r\n", nil
	}

	finalResponse := fmt.Sprintf("*%d\r\n", len(responses))
	for _, streamResponse := range responses {
		finalResponse += streamResponse
	}

	return finalResponse, nil
}

func handleIncr(key string, s *Server) (string, error) {
	if entry, ok := s.storage.cache[key]; ok {
		val, err := strconv.Atoi(entry.msg)
		if err != nil {
			return "-ERR value is not an integer or out of range\r\n", nil
		}

		incremented := strconv.Itoa(val + 1)

		entry.msg = incremented
		return fmt.Sprintf(":%s\r\n", incremented), nil
	}
	s.storage.cache[key] = NewEntry("1", 0)

	return ":1\r\n", nil
}
