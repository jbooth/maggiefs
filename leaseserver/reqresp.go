package leaseserver

import ()

const (
	LEASESERVER_PORT      = 1111
	OP_READLEASE          = iota
	OP_READLEASE_RELEASE  = iota
	OP_WRITELEASE         = iota
	OP_WRITELEASE_RELEASE = iota
	OP_WRITELEASE_COMMIT  = iota
	OP_CHECKLEASES        = iota

	STATUS_OK     byte = 0
	STATUS_ERR    byte = 1
	STATUS_WAIT   byte = 2
	STATUS_NOTIFY byte = 3
)

type request struct {
	Op      byte
	Leaseid uint64
	Inodeid uint64
	Reqno   uint32 // sent back with response so we know which request it was
}

type response struct {
	Reqno   uint32 // reqno that was sent with the request, 0 if a notify
	Leaseid uint64
	Inodeid uint64
	Status  byte // ok, err, or we're a notify
}
