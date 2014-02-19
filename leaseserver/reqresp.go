package leaseserver

import ()

const (
	LEASESERVER_PORT          = 1111
	OP_READLEASE         byte = 1
	OP_READLEASE_RELEASE byte = 2
	OP_NOTIFY            byte = 3
	OP_NOTIFY_DONE       byte = 4
	OP_CHECKLEASES       byte = 5
	OP_ACKNOWLEDGE       byte = 6 // used to respond to a notify
	OP_CLOSE             byte = 7

	STATUS_OK     byte = 0
	STATUS_ERR    byte = 1
	STATUS_WAIT   byte = 2
	STATUS_NOTIFY byte = 3
)

type request struct {
	Op      byte
	Leaseid uint64
	Inodeid uint64
	Reqno   uint64 // sent back with response so we know which request it was, overridden with ackID if an ack
}

type response struct {
	Reqno   uint64 // reqno that was sent with the request, overridden with ackID if a notify
	Leaseid uint64
	Inodeid uint64
	Status  byte // ok, err, or we're a notify
}
