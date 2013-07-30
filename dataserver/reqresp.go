package dataserver

import (
	"github.com/jbooth/maggiefs/maggiefs"
	"encoding/binary"
	"io"
	"net"
	"os"
	"syscall"
)

const (
	OP_READ  = uint8(0)
	OP_WRITE = uint8(1)
	STAT_OK  = uint8(0)
	STAT_ERR = uint8(1)
	STAT_NOBLOCK = uint8(2)
	STAT_BADVOLUME = uint8(3)
	STAT_BADOP = uint8(4)
)

// used to represent sockets that we want to do FD-based operations on, attaches addr information for debugging
type connFile struct {
  f *os.File
  LocalAddr string
  RemoteAddr string
}

// returns a new connFile representing this tcpConn.  Conn is closed.
func newConnFile(conn *net.TCPConn) (*connFile,error) {
  defer conn.Close()
  conn.SetNoDelay(true)
  conn.SetKeepAlive(true)
  localAddr := conn.LocalAddr().String()
  remoteAddr := conn.RemoteAddr().String()
  f,err := conn.File()
  if err != nil {
    return nil,err
  }
  // set to blocking
  syscall.SetNonblock(int(f.Fd()),false)
  return &connFile{f,localAddr,remoteAddr},nil
}

type RequestHeader struct {
	Op     uint8
	Blk    maggiefs.Block
	Pos    uint64
	Length uint32
}

func (r *RequestHeader) BinSize() int {
	return 13 + r.Blk.BinSize()
}

// doesn't bounds check, caller should check BinSize before calling this
func (r *RequestHeader) ToBytes(b []byte) int {
  b[0] = r.Op
  off := 1
  off += r.Blk.ToBytes(b[off:])
  binary.LittleEndian.PutUint64(b[off:],r.Pos)
  off += 8
  binary.LittleEndian.PutUint32(b[off:],r.Length)
  off += 4
  return off
}

func (r *RequestHeader) FromBytes(b []byte) int {
	r.Op = b[0]
	off := 1
	off += r.Blk.FromBytes(b[off:])
	r.Pos = binary.LittleEndian.Uint64(b[off:])
	off += 8
	r.Length = binary.LittleEndian.Uint32(b[off:])
	off += 4  
	return off
}

// writes its length and then itself
func (r *RequestHeader) WriteTo(w io.Writer) (n int, err error) {
	reqLen := r.BinSize()
	reqBuff := make([]byte,reqLen + 2)
	binary.LittleEndian.PutUint16(reqBuff,uint16(reqLen))
	r.ToBytes(reqBuff[2:])
	written := 0
	for ; written < (reqLen + 2) ; {
		writ,err := w.Write(reqBuff[written:])
		written += writ
		if err != nil {
			return written,err
		}
	}	
	return written,nil
}

func (req *RequestHeader) ReadFrom(r io.Reader) (n int, err error) {
	// read size
	reqLenBuff := [2]byte{}
	nRead := 0
	for ; nRead < 2 ; {
		n,err := r.Read(reqLenBuff[nRead:])
		if err != nil { return 0,err }
		nRead += n
	}
	nRead = 0
	reqLen := int(binary.LittleEndian.Uint16(reqLenBuff[:]))
	reqBuff := make([]byte,reqLen)
	for ; nRead < reqLen ; {
		n,err := r.Read(reqBuff[nRead:])
		if err != nil { return 0,err }
		nRead += n
	}
	req.FromBytes(reqBuff)
	return reqLen + 2,nil
}

type ResponseHeader struct {
	Stat uint8
}

func (r *ResponseHeader) BinSize() int {
	return 1
}

func (r *ResponseHeader) ToBytes(b []byte) int {
	b[0] = r.Stat
	return 1
}

func (r *ResponseHeader) FromBytes(b []byte) int {
	r.Stat = b[0]
	return 1
}

func (resp *ResponseHeader) ReadFrom(r io.Reader) (n int, err error) {
	buff := [1]byte{}
	nRead := 0
	for ; nRead < 1 ; {
		nRead,err = r.Read(buff[:])
		if err != nil {
			return 0,err
		}
	}
	resp.Stat = 0
	return 1,nil
}

func (resp *ResponseHeader) WriteTo(w io.Writer) (n int, err error) {
	buff := [1]byte{resp.Stat}
	nWrit := 0
	for ; nWrit < 1 ; {
		nWrit,err = w.Write(buff[:])
		if err != nil {
			return 0,err
		}
	}
	return 1,nil
}