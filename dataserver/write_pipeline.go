package dataserver

import (
	"sync"
)


type ClientPipeline struct {
	server Endpoint
	blk Block
	l *sync.Cond
	bytesInFlight int
	maxBytesInFlight int
	awaitingAck chan int // the number of bytes sent with each request awaiting an ack
}

func newClientPipeline(server Endpoint, blk Block,  maxBytesInFlight int) (*ClientPipeline,error) {
	ret := &clientPipeline {
		server,
		blk,
		sync.NewCond(new(sync.Mutex)),
		0,
		maxBytesInFlight,
		make(chan int, int(maxBytesInFlight/131072)), // we try to send 128kb per write so allow that many requests outstanding
		
	}
	
	// send WRITE_START
	// launch ack goroutine
	return ret,nil
}

func (c *ClientPipeline) 	Write(p []byte, pos uint64) (err error) {
	return nil
}

func (c *ClientPipeline) SyncAndClose() (err error) {
	return nil
}
	

// used by the server to manage a stateful write pipeline
type serverPipeline struct {
	client Endpoint
	nextInLine Endpoint
	vol *volume
	buff []byte
	
}

func newServerPipeline(client Endpoint, nextInLine Endpoint, 




