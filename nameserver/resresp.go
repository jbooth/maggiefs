package nameserver

import (
  "github.com/jbooth/maggiefs/maggiefs"
  "encoding/binary"
  "bytes"
)

const (
  OP_GETINODE int32 = iota
  OP_SETINODE int32 = iota
  OP_ADDBLOCK int32 = iota
  OP_RMBLOCK int32 = iota
  OP_EXTENDBLOCK int32 = iota
  
  STAT_OK byte = 0
  STAT_ERR byte = 1
  STAT_RETRY byte = 2
  
)

func fromInode(i *maggiefs.Inode) []byte {
  if i == nil { return []byte{} }
  ret := make([]byte,binary.Size(*i))
  binary.Write(bytes.NewBuffer(ret),binary.LittleEndian,i)
  return ret
}


func toInode(b []byte) *maggiefs.Inode {
  ret := &maggiefs.Inode{}
  binary.Read(bytes.NewBuffer(b),binary.LittleEndian,ret)
  return ret
}



type request struct {
  Op int32
  Inodeid uint64
  Generation uint64
  Body []byte // either an Inode or a BlockRequest, encoded using encoding/binary
}

type response struct {
  Status byte
  Body []byte // either an Inode or a Block
}
