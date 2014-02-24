package dataserver

import (
	"fmt"
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
	// encode header
	headerBytes := make([]byte, header.BinSize(), header.BinSize())
	header.ToBytes(headerBytes)

	// build iovecs
	var iovecs []syscall.Iovec
	if body != nil {
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
				Len:  uint64(len(body)),
			},
		}
	}
	// register our callback
	mod := header.Reqno % uint32(len(c.stripeLock))
	c.stripeLock[mod].Lock()
	c.callBacks[mod][header.Reqno] = onResp
	c.stripeLock[mod].Unlock()
	// send request
	_, _, errno := syscall.Syscall(
		syscall.SYS_WRITEV,
		uintptr(c.server.Fd()), uintptr(unsafe.Pointer(&iovecs[0])), uintptr(len(iovecs)))
	if errno != 0 {
		return os.NewSyscallError("writev", syscall.Errno(errno))
	}
	return nil
}

func (c *RawClient) handleResponses() {
	b := make([]byte, 5, 5)
	resp := ResponseHeader{}
	for {
		_, err := c.server.Read(b)
		if err != nil {
			fmt.Printf("Error! %s", err)
			return
		}
		resp.FromBytes(b)
		// TODO look at status
		mod := resp.Reqno % uint32(len(c.stripeLock))

		c.stripeLock[mod].Lock()
		subMap := c.callBacks[mod]
		cb := subMap[resp.Reqno]
		delete(subMap, resp.Reqno)
		c.stripeLock[mod].Unlock()
		cb(c.server)
	}
}
