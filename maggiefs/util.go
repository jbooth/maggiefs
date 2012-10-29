package maggiefs

import (
  "sync/atomic"
)

// deep copies an inode
func CopyInode(i *Inode) *Inode {
  ret := Inode{}
  ret.Inodeid = i.Inodeid
  ret.Generation = i.Generation
  ret. Ftype = i.Ftype
  ret.Length = i.Length
  ret.Mode = i.Mode
  ret.Mtime = i.Mtime
  ret.Ctime = i.Ctime
  ret.Nlink = i.Nlink
  ret. Uid = i.Uid
  ret.Gid = i.Gid
  ret.Symlinkdest = i.Symlinkdest
  ret.Children = make(map[string] Dentry)
  ret.Xattr = make(map[string] []byte)
  ret.Blocks = make([]Block,len(i.Blocks),len(i.Blocks))
  for i,b := range(i.Blocks) {
    ret.Blocks[i] = b
  }
  for n,c := range(i.Children) {
    ret.Children[n] = c
  }
  for n,v := range(i.Xattr) {
    newV := make([]byte, len(v),len(v))
    copy(newV,v)
    ret.Xattr[n] = newV
  }
  return &ret
}

// atomically adds incr to val, returns new val
func IncrementAndGet(val *uint64, incr uint64) uint64 {
  currVal := atomic.LoadUint64(val)
  for ; !atomic.CompareAndSwapUint64(val,currVal,currVal+incr) ; {
    currVal = atomic.LoadUint64(val)
  }
  return currVal + incr
}
