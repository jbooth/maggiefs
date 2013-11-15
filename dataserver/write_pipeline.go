package dataserver

import (
	"sync"
	"os"
	"github.com/jbooth/maggiefs/maggiefs"
	"fmt"
)


type ClientPipeline struct {
	dc *DataClient
	server Endpoint
	blk maggiefs.Block
	l *sync.Cond
	bytesInFlight int
	maxBytesInFlight int
	awaitingAck chan int // the number of bytes sent with each request awaiting an ack
	gotAllAcks chan bool // ack thread sends a single bool to this chan after receiving the last ack required
	closed bool // whether we've already been closed
}

func newClientPipeline(dc *DataClient, server Endpoint, blk maggiefs.Block,  maxBytesInFlight int) (*ClientPipeline,error) {
	ret := &ClientPipeline {
		dc,
		server,
		blk,
		sync.NewCond(new(sync.Mutex)),
		0,
		maxBytesInFlight,
		make(chan int, int(maxBytesInFlight/131072)), // we try to send 128kb per write so allow that many requests outstanding
		make(chan bool),
		false,
	}
	
	// send WRITE_START
	// launch ack goroutine
	return ret,nil
}

func (c *ClientPipeline) Write(p []byte, pos uint64) (err error) {
	c.l.L.Lock()
	if c.closed {
		return fmt.Errorf("ClientPipeline was already closed!")
	}
	// wait until there are enough bytes available
	for c.bytesInFlight + len(p) > c.maxBytesInFlight {
		c.l.Wait() 
	}
	defer c.l.L.Unlock()
	// send req header
	header := &RequestHeader{OP_WRITE_BYTES, c.blk, pos, uint32(len(p))}
	_,err = header.WriteTo(c.server)
	if err != nil {
		return fmt.Errorf("Error writing bytes: %s",err)
	}
	// send req bytes
	numWritten := 0
	for numWritten < len(p) {
		//	fmt.Printf("Writing bytes from pos %d, first byte %x\n", numWritten, p[numWritten])
		n, err := c.server.Write(p[numWritten:])
		if err != nil {
			return err
		}
		numWritten += n
	}
	// increament bytesInFlight
	c.bytesInFlight += numWritten
	c.awaitingAck <- numWritten
	return nil
}

func (c *ClientPipeline) SyncAndClose() (err error) {
	c.l.L.Lock()
	if c.closed { 
		c.l.L.Unlock()
		return nil 
	}
	c.closed = true
	c.l.L.Unlock()
	close(c.awaitingAck)
	<- c.gotAllAcks
	return nil	
}

func (c *ClientPipeline) ReadAcks() {
	resp := &ResponseHeader{}
	for numBytesAck := range c.awaitingAck {
		// read resp header
		_, err := resp.ReadFrom(c.server)
		if err != nil || resp.Stat != STAT_OK {
			fmt.Printf("Error code %d from DN while acking write, error: %s\n", resp.Stat,err)
		}
		c.l.L.Lock()
		c.bytesInFlight -= numBytesAck
		c.l.Broadcast()
		c.l.L.Unlock()
	}
	// awaitingAck chan is closed and we've received all syncs so signal done chan
	c.gotAllAcks <- true
}
	

// used by the server to manage a stateful write pipeline
type serverPipeline struct {
	client Endpoint
	nextInLine Endpoint
	f *os.File
	buff []byte
	
}




