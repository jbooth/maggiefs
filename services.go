package maggiefs

import (
  "syscall"
)
type NameService interface {
  
  Format() (err error)
  GetChild(parentid uint64, name string) (i Inode, err error)
  GetInode(nodeid uint64) (i Inode, err error)
  StatFs() (statfs syscall.Statfs_t, err error)
  // persists a new inode to backing store
  AddInode(node Inode) (id uint64, err error)
  // updates existing inode
  SaveInode(node Inode) (err error)
  // acquires write lock
  WriteLock(nodeid uint64) (lock WriteLock, err error)
  // queues deletion for an entry
  QueueDelete(node Inode) (err error)
}

type WriteLock interface {
  Unlock()
}

type DataService interface {
  Read(blk Block) (conn BlockReader, err error)

  Write(blk Block) (conn BlockWriter, err error)

  Delete(blk Block) (err error)
}

// represents a session of interacting with a block of a file
// each 16MB block consists of 4096 pages of 4096 bytes apiece
// sessions are navigated by seeking to a page number and then 
// reading or writing full pages of 4096 bytes
type BlockReader interface {

  // reads  some bytes
  Read(start uint32, length uint32, p []byte) (err error)


  // closes or returns to pool
  Close() (err error)
}

type BlockWriter interface {
  Append(p []byte) (err error)
  Close() (err error)
}
