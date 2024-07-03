package protocol

func Handshake(r *Replica) {
	sendPing(r)
}

func sendPing(r *Replica) {
	r.C.Write("*1\r\n$4\r\nPING\r\n")
}
