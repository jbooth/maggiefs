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

func FromBlock(b *Block) []byte {
  ret := make([]byte, binary.Size(b))
  binary.Write(bytes.NewBuffer(ret), binary.LittleEndian, b)
  return ret
}

func ToBlock(b []byte) *Block {
  ret := &Block{}
  binary.Read(bytes.NewBuffer(b), binary.LittleEndian,ret)
  return ret
}