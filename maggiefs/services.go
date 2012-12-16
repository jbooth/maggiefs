package maggiefs

import (
  "syscall"
  "errors"
)

var (
  E_EXISTS = errors.New("file exists!")
)

type LeaseService interface {
  
  // acquires the write lease for the given inode
  // only one client may have the writelease at a time, however it is pre-emptable in case 
  // a higher priority process (re-replication etc) needs this lease.
  // on pre-emption, the supplied commit() function will be called
  // pre-emption will not happen while WriteLease.ShortTermLock() is held, however that lock should 
  // not be held for the duration of anything blocking
  WriteLease(nodeid uint64) (l WriteLease, err error)
  
  // takes out a lease for an inode, this is to keep the posix convention that unlinked files
  // aren't cleaned up until they've been closed by all programs
  // also registers a callback for when the node is remotely changed, this will be triggered
  // upon the file changing *unless* we've cancelled this lease.  Recommend 
  ReadLease(nodeid uint64) (l ReadLease, err error)
  
  // returns a chan which will contain an event every time any inode in the system is changed
  // used for cache coherency
  // the fuse client runs a goroutine reading all changes from this chan
  GetNotifier() chan uint64
  
  // blocks until all leases are released for the given node
  WaitAllReleased(nodeid uint64) error
}

type WriteLease interface {
  // lets go of lock
  Release() error
  // commits changes, sending uint64(now()) to all readers registered for this inode id.  May yield and reacquire lock.
  Commit() error
}

type ReadLease interface {
  Release() error
}


type NameService interface {
  GetInode(nodeid uint64) (i *Inode, err error)
  StatFs() (statfs syscall.Statfs_t, err error)
  // persists a new inode to backing store
  AddInode(node Inode) (id uint64, err error)
  // atomically mutates an inode, optimization over WriteLock for small operations
  Mutate(nodeid uint64, mutator func(inode *Inode) error) (newNode *Inode, err error)
  // Links the given child to the given parent, with the given name
  Link(parent uint64, child uint64, name string) (newNode *Inode, err error)
  // Unlinks the child with the given name
  Unlink(parent uint64, name string) (newNode *Inode, err error)
  // add a block to the end of a file, returns new block
  AddBlock(nodeid uint64, length uint32) (newBlock Block, err error)
  // extend a block and the relevant inode
  ExtendBlock(nodeid uint64, blockId uint64, delta uint32) (newBlock Block, err error)
}



type DataService interface {
  Read(blk Block) (conn BlockReader, err error)

  Write(blk Block) (conn BlockWriter, err error)

  AddBlock(id uint64) error
  RmBlock(id uint64) error
  ExtendBlock(id uint64, delta uint32) error
}

// represents a session of interacting with a block of a file
// sessions are navigated by seeking to a page number and then 
// reading or writing full pages of 4096 bytes
type BlockReader interface {

  // reads a page
  ReadPage(p []byte) (err error)

  // seeks to a page
  SeekPage(pageNum int) error

  // lists the current/next page number (page num * 4096 is position within block)
  CurrPageNum() int

  // closes or returns to pool
  Close() (err error)
}

type BlockWriter interface {
  // return which block id this writer is writing
  BlockId() uint64
  // writes a whole page
  // can expand block by one page or overwrite existing page
  WritePage(p []byte, pageNum int) error
  // writes a subpage
  Write(p []byte, pageNum int, off int, length int) error
  // flushes changes to system
  Sync() (err error)
  // flushes and closes this writer
  Close() (err error)
}

// interface exposed from datanodes to namenode
// this is typically over a single socket which 
type NameDataIface interface {

  HeartBeat() (DataNodeStat, error)
  AddBlock(id uint64) error
  RmBlock(id uint64) error
  ExtendBlock() error  
}

type DataNodeStat struct {
  TotalBytes uint64
  BytesUsed uint64
  NumBlocks uint64
}
