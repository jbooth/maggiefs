package maggiefs

import (
  "syscall"
)

type NameService interface {
  Format() (err error)
  GetChild(parentid uint64, name string) (i *Inode, err error)
  GetInode(nodeid uint64) (i *Inode, err error)
  StatFs() (statfs syscall.Statfs_t, err error)
  // persists a new inode to backing store
  AddInode(node Inode) (id uint64, err error)
  // acquires write lock
  WriteLock(nodeid uint64) (lock WriteLock, err error)
  // queues deletion for an entry, optimization instead of waiting for GC
  MarkGC(nodeid uint64) (err error)
  // atomically mutates an inode, optimization over WriteLock for small operations
  Mutate(nodeid uint64, mutator func(inode *Inode) error) (newNode *Inode, err error)
  // add a block to the end of a file, returns new inode def
  AddBlock(nodeid uint64) (newNode *Inode, err error)
  // takes out a lease for an inode, this is to keep the posix convention that unlinked files
  // aren't cleaned up until they've been closed by all programs
  Lease(nodeid uint64) (ls Lease, err error)
}

type WriteLock interface {
  Unlock()
}

type Lease interface {
  Release()
}

type DataService interface {
  Read(blk Block) (conn BlockReader, err error)

  Write(blk Block) (conn BlockWriter, err error)

  Delete(blk Block) (err error)
}

// represents a session of interacting with a block of a file
// sessions are navigated by seeking to a page number and then 
// reading or writing full pages of 4096 bytes
type BlockReader interface {

  // reads a page
  ReadPage(p []byte) (err error)

  // seeks to a page
  SeekPage(pageNum int)

  // lists the current page number (page num * 4096 is position within block)
  CurrPageNum() int

  // closes or returns to pool
  Close() (err error)
}

type BlockWriter interface {
  // writes a whole page
  // can expand block by one page or overwrite existing page
  WritePage(p []byte, pageNum int)
  // writes a subpage
  Write(p []byte, pageNum int, off int, len int)
  // flushes changes to system
  Sync() (err error)
  // flushes and closes this writer
  Close() (err error)
}
