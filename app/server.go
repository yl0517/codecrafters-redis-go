package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"

	"github.com/codecrafters-io/redis-starter-go/protocol"
	"github.com/jessevdk/go-flags"
)

var opts struct {
	PortNum   string `long:"port" description:"Port Number" default:"6379"`
	ReplicaOf string `long:"replicaof" description:"Replica of <MASTER_HOST> <MASTER_PORT>" default:""`
}

func NewReplica(conn *protocol.Connection) *protocol.Replica {
	return &protocol.Replica{
		C:                conn,
		RepInfo:          opts.ReplicaOf,
		MasterReplid:     GenerateReplid(),
		MasterReplOffset: "0",
	}
}

func GenerateReplid() string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz1234567890"

	b := make([]byte, 40)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	_, err := flags.Parse(&opts)
	if err != nil {
		fmt.Println("flags.Parse failed:", err.Error())
	}

	l, err := net.Listen("tcp", "0.0.0.0:"+opts.PortNum)
	if err != nil {
		fmt.Println("Failed to bind to port")
		os.Exit(1)
	}

	if opts.ReplicaOf != "" {
		protocol.Handshake(opts.ReplicaOf)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go handleConnection(conn)
	}
}

func handleConnection(c net.Conn) {
	conn := protocol.NewConnection(c)

	defer conn.Close()

	for {
		numElem, err := conn.GetLine()
		if err != nil {
			fmt.Println("conn.GetLine() failed: ", err.Error())
			return
		}

		len, err := protocol.GetArrayLength(numElem)
		if err != nil {
			fmt.Println("protocol.GetLength() failed: ", err.Error())
			return
		}

		var request []string

		for i := 0; i < len; i++ {
			line, err := conn.GetLine()
			if err != nil {
				fmt.Println("conn.GetLine() failed: ", err.Error())
				return
			}

			len, err := protocol.GetBulkStringLength(line)
			if err != nil {
				fmt.Println("protocol.GetBulkStringLength() failed: ", err.Error())
				return
			}

			s, err := conn.GetLine()
			if err != nil {
				fmt.Println("conn.GetLine() failed: ", err.Error())
				return
			}

			err = protocol.VerifyBulkStringLength(s, len)
			if err != nil {
				fmt.Println("protocol.VerifyBulkStringLength() failed: ", err.Error())
				return
			}

			request = append(request, s)
		}

		protocol.HandleRequest(NewReplica(conn), request)
	}
}
