package protocol

import (
	"math/rand"
	"strings"
)

// Opts represents the options given by user
type Opts struct {
	PortNum    string `short:"p" long:"port" description:"Port Number" default:"6379"`
	ReplicaOf  string `long:"replicaof" description:"Replica of <MASTER_HOST> <MASTER_PORT>"`
	Dir        string `long:"dir" description:"Path to the directory where RDB file is stored"`
	Dbfilename string `long:"dbfilename" description:"name of RDB file"`

	Role       string
	ReplID     string
	MasterHost string
	MasterPort string
}

// Config configurates the options given by user
func (o *Opts) Config() {
	o.Role = "master"
	o.ReplID = generateReplid()

	if o.ReplicaOf != "" {
		o.Role = "slave"
		o.ReplID = ""

		hostAndPort := strings.Split(o.ReplicaOf, " ")
		o.MasterHost = hostAndPort[0]
		o.MasterPort = hostAndPort[1]
	}
}

// generateReplid create a pseudo random alphanumeric string of 40 characters
func generateReplid() string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz1234567890"

	b := make([]byte, 40)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// AddSlave adds a slave to the list of slaves stored by master
func (s *Server) AddSlave(conn *Connection) {
	if s.opts.Role == "master" {
		s.slaves.AddSlave(conn.conn.RemoteAddr(), conn)
	}
}
