package dataserver

import (
	"github.com/jbooth/maggiefs/maggiefs"
	"encoding/binary"
	"bytes"
)

const (
	OP_READ  = uint8(0)
	OP_WRITE = uint8(1)
	STAT_OK  = uint8(0)
	STAT_ERR = uint8(1)
)

type RequestHeader struct {
	Op     uint8
	Blk    maggiefs.Block
	Pos    uint64
	Length uint32
}

func (r *RequestHeader) ToBytes() []byte {
	if r == nil {
		return []byte{}
	}
	ret := make([]byte, binary.Size(*r))
	binary.Write(bytes.NewBuffer(ret), binary.LittleEndian, r)
	return ret
}

type ResponseHeader struct {
	Stat uint8
	Err  string
}
