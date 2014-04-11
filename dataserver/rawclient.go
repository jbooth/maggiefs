package dataserver

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"unsafe"
)

type RawClient struct {
	server     *os.File
	reqnoCtr   *uint32
	callBacks  map[uint32]map[uint32]RespCb
	stripeLock []*sync.Mutex // guards callback map
	writeLock  *sync.Mutex   // guard socket
	closed     bool          // true if our socket is closed
}

// on receiving a valid response, we call onResponse
// if any onResponse returns an error (or any other error happens),
// we call onErr for all pending callbacks
// NOTE:  we process these single-threaded for each connection, so don't block
// too long.  spin a goroutine if you need to.
type RespCb interface {
	OnResponse(s *os.File) error
	OnErr(error)
}

// wraps the conn using the provided numStripes to hash our callbacks into
// calls onDie() if/when we have an error communicating with conn
func NewRawClient(conn *net.TCPConn, numStripes int) (*RawClient, error) {
	stripeLock := make([]*sync.Mutex, numStripes, numStripes)
	callBacks := make(map[uint32]map[uint32]RespCb)
	for i := 0; i < numStripes; i++ {
		stripeLock[i] = new(sync.Mutex)
		callBacks[uint32(i)] = make(map[uint32]RespCb)
	}
	f, err := conn.File()
	if err != nil {
		return nil, err
	}
	conn.Close()
	syscall.SetNonblock(int(f.Fd()), false)
	ctr := uint32(0)
	ret := &RawClient{f, &ctr, callBacks, stripeLock, new(sync.Mutex), false}
	go ret.handleResponses()
	return ret, nil
}

// sends a request, calling onResp asynchronously when we receive a response
// returns error if we had a problem sending
func (c *RawClient) DoRequest(header RequestHeader, body []byte, onResp RespCb) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()
	if c.closed {
		return fmt.Errorf("RawClient: connection already closed!")
	}
	// set reqno
	header.Reqno = atomic.AddUint32(c.reqnoCtr, 1)
	// encode header
	headerLen := uint16(header.BinSize())
	headerBytes := make([]byte, headerLen+2, headerLen+2)
	binary.LittleEndian.PutUint16(headerBytes, headerLen)
	header.ToBytes(headerBytes[2:])

	// build iovecs
	var iovecs []syscall.Iovec
	if body != nil && len(body) > 0 {
		iovecs = []syscall.Iovec{
			syscall.Iovec{
				Base: &headerBytes[0],
				Len:  uint64(len(headerBytes)),
			},
			syscall.Iovec{
				Base: &body[0],
				Len:  uint64(len(body)),
			},
		}
	} else {
		iovecs = []syscall.Iovec{
			syscall.Iovec{
				Base: &headerBytes[0],
				Len:  uint64(len(headerBytes)),
			},
		}
	}
	// register our callback
	mod := header.Reqno % uint32(len(c.stripeLock))
	c.stripeLock[mod].Lock()
	c.callBacks[mod][header.Reqno] = onResp
	c.stripeLock[mod].Unlock()
	totalBytes := uint64(0)
	for _, iovec := range iovecs {
		totalBytes += iovec.Len
	}
	// send request
	ret1, _, errno := syscall.Syscall(
		syscall.SYS_WRITEV,
		uintptr(c.server.Fd()), uintptr(unsafe.Pointer(&iovecs[0])), uintptr(len(iovecs)))
	if errno != 0 {
		log.Printf("Error calling writev in rawclient!  %s", syscall.Errno(errno))
		err := os.NewSyscallError("writev", syscall.Errno(errno))
		c.die(err)
		return err
	}
	if totalBytes > uint64(ret1) {
		err := fmt.Errorf("Writev wrote less than expected!  %d < %d", ret1, totalBytes)
		c.die(err)
		return err
	}
	return nil
}

func (c *RawClient) handleResponses() {
	b := make([]byte, 5, 5)
	resp := ResponseHeader{}
	for {
		nRead := 0
		for nRead < 5 {
			n, err := c.server.Read(b[nRead:])
			if err != nil {
				log.Printf("Error reading response from socket %s! %s\n", c.server, err)
				c.die(err)
				return
			}
			nRead += n
		}
		resp.FromBytes(b)
		// TODO look at status
		mod := resp.Reqno % uint32(len(c.stripeLock))
		c.stripeLock[mod].Lock()
		subMap := c.callBacks[mod]
		cb := subMap[resp.Reqno]
		delete(subMap, resp.Reqno)
		c.stripeLock[mod].Unlock()
		if cb == nil {
			log.Printf("RawClient.HandleResponses: Nil callback for reqno %d on client %s\n", resp.Reqno, c.server)
		}
		err := cb.OnResponse(c.server)
		if err != nil {
			cb.OnErr(err)
			c.writeLock.Lock()
			defer c.writeLock.Unlock()
			c.die(err)
			return
		}
	}
}

// tells all callbacks we had an error state
// c.writeLock should be held prior to calling this
func (c *RawClient) die(err error) {
	if c.closed {
		// already closed, just return
		return
	}
	c.closed = true
	for _, subMap := range c.callBacks {
		for k2, cb := range subMap {
			cb.OnErr(err)
			delete(subMap, k2)
		}
	}
}
