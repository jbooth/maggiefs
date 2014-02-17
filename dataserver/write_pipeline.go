package dataserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"io"
	"os"
	"sync"
)

type ClientPipeline struct {
	server           Endpoint
	blk              maggiefs.Block
	l                *sync.Cond
	bytesInFlight    int
	maxBytesInFlight int
	awaitingAck      chan int  // the number of bytes sent with each request awaiting an ack
	gotAllAcks       chan bool // ack thread sends a single bool to this chan after receiving the last ack required
	closed           bool      // whether we've already been closed
	onDone           func()    // called after we're finished IFF there is no error condition, used to return server endpoint to pool
}

func newClientPipeline(server Endpoint, blk maggiefs.Block, maxBytesInFlight int, onDone func()) (*ClientPipeline, error) {
	ret := &ClientPipeline{
		server,
		blk,
		sync.NewCond(new(sync.Mutex)),
		0,
		maxBytesInFlight,
		make(chan int, int(maxBytesInFlight/131072)), // we try to send 128kb per write so allow that many requests outstanding
		make(chan bool),
		false,
		onDone,
	}

	// send WRITE_START to set up pipeline
	header := &RequestHeader{OP_START_WRITE, blk, 0, 0}
	_, err := header.WriteTo(server)
	// launch ack goroutine
	go ret.readAcks()
	return ret, err
}

func (c *ClientPipeline) Write(p []byte, pos uint64) (err error) {
	// fmt.Printf("client pipeline write acquiring lock\n")
	c.l.L.Lock()
	defer c.l.L.Unlock()
	if c.closed {
		return fmt.Errorf("ClientPipeline was already closed!")
	}
	// wait until there are enough bytes available
	for c.bytesInFlight+len(p) > c.maxBytesInFlight {
		//fmt.Printf("client pipeline write waiting till bytes freed up\n")
		c.l.Wait()
	}
	// send req header
	header := &RequestHeader{OP_WRITE_BYTES, c.blk, pos, uint32(len(p))}
	//fmt.Printf("Client pipeline writing header %+v\n", header)
	_, err = header.WriteTo(c.server)
	if err != nil {
		return fmt.Errorf("Error writing bytes: %s", err)
	}
	// send req bytes
	//fmt.Printf("Client pipeline writing bytes\n")
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
	req := RequestHeader{OP_COMMIT_WRITE, c.blk, 0, 0}
	_, err = req.WriteTo(c.server)
	if err != nil {
		return err
	}
	c.closed = true
	c.l.L.Unlock()
	close(c.awaitingAck)
	<-c.gotAllAcks
	return err
}

func (c *ClientPipeline) readAcks() {
	resp := &ResponseHeader{}
	haveErr := false
	for numBytesAck := range c.awaitingAck {
		// read resp header
		_, err := resp.ReadFrom(c.server)
		if err != nil || resp.Stat != STAT_OK {
			haveErr = true
			fmt.Printf("Error code %d from DN while acking write, error: %s\n", resp.Stat, err)
		}
		c.l.L.Lock()
		c.bytesInFlight -= numBytesAck
		c.l.Broadcast()
		c.l.L.Unlock()
	}
	// awaitingAck chan is closed and we've received all syncs so signal done chan
	c.gotAllAcks <- true

	// if we got this far, return connection
	if !haveErr {
		c.onDone()
	} else {
		c.server.Close()
	}
}

// used by the server to manage a stateful write pipeline
type serverPipeline struct {
	client           Endpoint
	nextInLine       Endpoint
	remainingVolumes []uint32 // list of volumes with ourself removed, forwarded to rest of chain
	volForBlock      *volume
	blockId          uint64
	buff             []byte
	ackRequired      chan bool // channel representing the number of acks we need to receive, ack() reads from this
	allAcksReceived  chan bool // ack() sends to this exactly once when finished
}

// RequestHeader's block member should already have our volume removed, so it just contains remainingVolumes
func newServerPipeline(client Endpoint, nextInLine Endpoint, req *RequestHeader, v *volume, blockId uint64) *serverPipeline {
	// forward request to nextInLine
	if nextInLine != nil {
		req.WriteTo(nextInLine)
	}

	// construct ourselves
	return &serverPipeline{
		client,
		nextInLine,
		req.Blk.Volumes,
		v,
		blockId,
		maggiefs.GetBuff(),
		make(chan bool, 64),
		make(chan bool),
	}
}

// pulls requests from the client,
// in this situation we just received OP_START_WRITE and should receive a series of OP_WRITE_BYTES followed by a single OP_COMMIT_WRITE
func (s *serverPipeline) run() error {
	req := &RequestHeader{}
PIPELINE:
	for {
		// read header
		//fmt.Printf("Server pipeline running, trying to read req header\n")
		_, err := req.ReadFrom(s.client)
		if err != nil {
			return fmt.Errorf("Err serving conn for pipelined write %s : %s\n", s.client.String(), err.Error())
		}
		// if this is a sync request, break the loop and sync
		if req.Op == OP_COMMIT_WRITE {
			//fmt.Printf("Breaking server side pipeline\n")
			// forward the op
			if s.nextInLine != nil {
				req.Blk.Volumes = s.remainingVolumes
				_, err = req.WriteTo(s.nextInLine)
				if err != nil {
					return err
				}
			}
			break PIPELINE
		}
		// normal write
		//fmt.Printf("Handling server side write, length %d\n", req.Length)
		// all writes shuold be 128kb max, subset our 128k buffer to size
		myBuf := s.buff[:int(req.Length)]
		// read bytes
		_, err = io.ReadFull(s.client, myBuf)
		if err != nil {
			return fmt.Errorf("Error reading from conn for pipelined write %s : %s\n", s.client.String(), err.Error())
		}
		// send to nextInLine, if appropriate
		if s.nextInLine != nil {
			// forward header
			req.Blk.Volumes = s.remainingVolumes
			// TODO could optimize these into a single buffered write
			_, err = req.WriteTo(s.nextInLine)
			if err != nil {
				return fmt.Errorf("Error writing header to nextInLine in pipelined write %s : %s", s.nextInLine.String(), err)
			}

			// forward bytes
			_, err = s.nextInLine.Write(myBuf)
			if err != nil {
				return fmt.Errorf("Error writing to nextInLine in pipelined write %s : %s", s.nextInLine.String(), err)
			}
		}
		// write to disk
		err = s.volForBlock.withFile(s.blockId, func(f *os.File) error {
			_, e1 := f.WriteAt(myBuf, int64(req.Pos))
			return e1
		})
		if err != nil {
			return fmt.Errorf("Error flushing bytes to disk in pipelined write : %s", err)
		}
		if err != nil {
			// send error back to previous
			resp := &ResponseHeader{STAT_ERR}
			_, _ = resp.WriteTo(s.client)
			return err
		}
		if s.nextInLine != nil {
			// if we're not the last, mark ack required
			s.ackRequired <- true
		} else {
			// if we are the last, send ack, can't wait for anyone
			resp := &ResponseHeader{STAT_OK}
			resp.WriteTo(s.client)
		}
	}

	// fsync

	err := s.volForBlock.withFile(s.blockId, func(f *os.File) error {
		e1 := f.Sync()
		return e1
	})
	close(s.ackRequired)
	<-s.allAcksReceived
	//fmt.Printf("server pipeline returning")
	return err
}

// runs in a loop processing acks from nextInLine and passing them back to client
func (s *serverPipeline) ack() error {
	resp := &ResponseHeader{}
	// we'll get an entry in ackRequired for every write we send
	for _ = range s.ackRequired {
		// pull ack from socket
		_, err := resp.ReadFrom(s.nextInLine)
		if resp.Stat != STAT_OK {
			return fmt.Errorf("Error code %d from remote DN on ack", resp.Stat)
		}
		if err != nil {
			return fmt.Errorf("Network error receiving ack from pipelined write: %s", err)
		}
		// forward to our client
		_, err = resp.WriteTo(s.client)
		if err != nil {
			return fmt.Errorf("Network error forwarding ack back to client from pipelined write: %s", err)
		}
	}
	// ackRequired is closed, shut it down
	s.allAcksReceived <- true
	return nil
}
