package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/protocol"
	"github.com/jessevdk/go-flags"
)

func main() {
	var opts protocol.Opts

	_, err := flags.Parse(&opts)
	if err != nil {
		fmt.Println("flags.Parse failed:", err.Error())
	}

	opts.Config()

	if opts.Role != "master" {
		go connectToMaster(opts)
	}

	runMaster(opts)
}

func runMaster(o protocol.Opts) {
	l, err := net.Listen("tcp", "0.0.0.0:"+o.PortNum)
	if err != nil {
		fmt.Println("Failed to bind to port")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		c := protocol.NewConnection(conn)
		server := protocol.NewServer(c, o)

		go server.Handle()
	}
}

// handleReplConnection handles replication
func connectToMaster(o protocol.Opts) {
	addr := fmt.Sprintf("%s:%s", o.MasterHost, o.MasterPort)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("net.Dial() failed: ", err.Error())
	}

	c := protocol.NewConnection(conn)
	server := protocol.NewServer(c, o)

	err = server.Handshake()
	if err != nil {
		fmt.Println("Handshake failed:", err.Error())
		return
	}

	server.Handle()
}
