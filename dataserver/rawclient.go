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
	callBacks  map[uint32]map[uint32]func(s *os.File)
	stripeLock []*sync.Mutex
}

func NewRawClient(host *net.TCPAddr, numStripes int) (*RawClient, error) {
	stripeLock := make([]*sync.Mutex, numStripes, numStripes)
	callBacks := make(map[uint32]map[uint32]func(s *os.File))
	for i := 0; i < numStripes; i++ {
		stripeLock[i] = new(sync.Mutex)
		callBacks[uint32(i)] = make(map[uint32]func(s *os.File))
	}
	conn, err := net.DialTCP("tcp", nil, host)
	conn.SetNoDelay(true)
	conn.SetKeepAlive(true)
	conn.SetWriteBuffer(5 * 1024 * 1024)
	conn.SetReadBuffer(5 * 1024 * 1024)
	f, err := conn.File()
	if err != nil {
		return nil, err
	}
	conn.Close()
	syscall.SetNonblock(int(f.Fd()), false)
	ctr := uint32(0)
	ret := &RawClient{f, &ctr, callBacks, stripeLock}
	go ret.handleResponses()
	return ret, nil
}

// sends a request, calling onResp asynchronously when we receive a response
// returns error if we had a problem sending
func (c *RawClient) DoRequest(header RequestHeader, body []byte, onResp func(s *os.File)) error {
	// set reqno
	header.Reqno = atomic.AddUint32(c.reqnoCtr, 1)
	log.Printf("DataServer.RawClient: executing request with header %+v to sock %s\n", header, c.server)
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
	log.Printf("DataServer.RawClient: Registered callback %d on client %s\n", header.Reqno, c.server)
	totalBytes := uint64(0)
	for _, iovec := range iovecs {
		totalBytes += iovec.Len
	}
	// send request
	ret1, ret2, errno := syscall.Syscall(
		syscall.SYS_WRITEV,
		uintptr(c.server.Fd()), uintptr(unsafe.Pointer(&iovecs[0])), uintptr(len(iovecs)))
	if errno != 0 {
		log.Printf("Error calling writev in rawclient!  %s", syscall.Errno(errno))
		return os.NewSyscallError("writev", syscall.Errno(errno))
	}
	if totalBytes > uint64(ret1) {
		return fmt.Errorf("Writev wrote less than expected!  %d < %d", ret1, totalBytes)
	}
	log.Printf("Writev tried to write %d total bytes, returned args %d , %d\n", totalBytes, ret1, ret2)
	return nil
}

func (c *RawClient) handleResponses() {
	b := make([]byte, 5, 5)
	resp := ResponseHeader{}
	for {
		log.Printf("RawClient.HandleResponses reading from %s\n", c.server)
		nRead := 0
		for nRead < 5 {
			n, err := c.server.Read(b[nRead:])
			if err != nil {
				log.Printf("Error reading response from socket %s! %s\n", c.server, err)
				return
			}
			nRead += n
		}
		resp.FromBytes(b)
		log.Printf("RawClient.HandleResponses got resp %+v from sock %s\n", resp, c.server)
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
		cb(c.server)
	}
}
