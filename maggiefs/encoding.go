package maggiefs

import (
  "encoding/binary"
  "bytes"
)


func FromInode(i *Inode) []byte {
  if i == nil {
    return []byte{}
  }
  ret := make([]byte, binary.Size(*i))
  binary.Write(bytes.NewBuffer(ret), binary.LittleEndian, i)
  return ret
}

func ToInode(b []byte) *Inode {
  ret := &Inode{}
  binary.Read(bytes.NewBuffer(b), binary.LittleEndian, ret)
  return ret
}
