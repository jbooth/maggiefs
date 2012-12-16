package nameserver

import (
  "github.com/jbooth/maggiefs/maggiefs"
  "encoding/binary"
  "bytes"
)

const (
  OP_GETINODE int32 = iota
  OP_SETINODE int32 = iota
  OP_LINK int32 = iota
  OP_UNLINK int32 = iota
  OP_ADDBLOCK int32 = iota
  OP_RMBLOCK int32 = iota
  OP_EXTENDBLOCK int32 = iota
  
  STAT_OK byte = 0
  STAT_ERR byte = 1
  STAT_RETRY byte = 2
  STAT_E_EXISTS byte = 3 // used for link operations
  STAT_E_ISDIRgit
  
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
  Body []byte // either an Inode or one of the request types below
}

type linkReqBody struct {
  ChildId uint64
  Name string
  Force bool // if a child already exists, do we want to force it or return E_EXISTS
}

func toLinkReq(b []byte) linkReqBody {
  ret := linkReqBody{}
  binary.Read(bytes.NewBuffer(b),binary.LittleEndian,&ret)
  return ret
}

func fromLinkReq(l linkReqBody) []byte {
  ret := make([]byte,binary.Size(l))
  binary.Write(bytes.NewBuffer(ret),binary.LittleEndian,l)
  return ret
} 



type unlinkReqBody struct {
  name string
}

type response struct {
  Status byte
  Body []byte // either an Inode or a Block
}
