package main

import (
	"fmt"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/protocol"
	"github.com/jessevdk/go-flags"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	var opts protocol.Opts

	_, err := flags.Parse(&opts)
	if err != nil {
		fmt.Println("flags.Parse failed:", err.Error())
	}

	opts.Config()

	if opts.Role != "master" {
		connectToMaster(opts)
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

		go handleMasterConnection(conn, o)
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
	protocol.Handshake(c, o)
}

func handleMasterConnection(c net.Conn, o protocol.Opts) {
	conn := protocol.NewConnection(c)
	server := protocol.NewServer(conn, o)

	defer conn.Close()

	for {
		request, err := conn.Read()
		if err != nil {
			fmt.Printf("conn.Read() failed: %v\n", err)
			return
		}

		err = protocol.HandleRequest(server, request)
		if err != nil {
			fmt.Printf("protocol.HandleRequest() failed: %v\n", err)
		}
	}
}
