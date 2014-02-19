package mrpc

import (
	"encoding/binary"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
)

type RawClient struct {
	server              *os.File
	reqnoCtr            *uint64
	callBacks map[uint64]map[uint64]func(s *os.File)
	stripeLock          []*sync.Mutex
}

func NewRawClient(host *net.TCPAddr, numStripes int) (*RawClient,error) {
	stripeLock := make([]*sync.RWMutex, numStripes, numStripes)
	callBacks := make(map[uint64]map[uint64]func(s *os.File))
	for i := 0; i < numStripes; i++ {
		stripeLock[i] = new(sync.Mutex)
		callBacks[uint64(i)] = make(map[uint64]func(s *os.File))
	}
	conn,err := net.DialTCP("tcp",nil,host)
	conn.SetNoDelay(true)
	conn.SetKeepAlive(true)
	conn.SetWriteBuffer(5 * 1024 * 1024)
	conn.SetReadBuffer(5 * 1024 * 1024)
	f,err := conn.File()
	if err != nil {
		return nil,err
	}
	conn.Close()
	syscall.SetNonblock(int(f.Fd()), false)
	ctr := uint64(0)
	return &RawClient{f,&ctr,callBacks,stripeLock},nil
}

// sends a request, calling onResp asynchronously when we receive a response
// returns error if we had a problem sending
func (c *RawClient) DoRequest(opcode byte, args [][]byte, onResp func(s *os.File)) error {
	header := ReqHeader{
		Opcode: opcode, 
		Reqno: atomic.AddUint64(c.reqnoCtr, 1), 
		Numargs: len(args)
	}
	// encode header
	headerBytes := make([]byte,13,13)
	headerBytes[0] = header.Opcode
	binary.LittleEndian.PutUint64(headerBytes[1:],header.Reqno)
	binary.LittleEndian.PutUint32(headerBytes[9:],header.Numargs)
	
	// encode args
	argLenBytes := make([]byte,len(args) * 4, len(args) * 4)
	for idx,arg := range args {
		binary.LittleEndian.PutUint32(argLenBytes[idx*4:],uint32(len(arg)))
	}
	// build iovecs
	iovecs := make([]syscall.Iovec, len(args) + 2, len(args) + 2)
	iovecs[0].Base = &headerBytes[0]
	iovecs[0].Len = len(headerBytes)
	iovecs[1].Base = &argLenBytes[0]
	iovecs[1].Len = len(argLenBytes)
	for idx,arg := range args {
		iovecs[idx+2].Base = &arg[0]
		iovecs[idx+2].Len = len(arg)
	}
	// register our callback
	mod := header.Reqno % len(c.stripeLock)
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
	b := make([]byte, 4, 4)
	for {
		err := c.f.Read(b)
		if err != nil {
			fmt.Printf("Error! %s",err)
			return
		}
		reqNo := binary.LittleEndian.Uint32(b)
		mod := reqNo % len(c.stripeLock)
		
		c.stripeLock[mod].Lock()
		subMap := c.callBacks[mod]
		cb := subMap[reqNo]
		delete(subMap,reqNo)
		c.stripeLock[mod].Unlock()
		cb(c.f)
	}
}

type ReqHeader struct {
	Opcode  byte
	Reqno   uint64
	Numargs uint32
}

type RespHeader struct {
	Reqno uint64 // used to match response to a callback
	Stat uint8 // used to indicate error status
	RespLen uint32 // used to 
}
