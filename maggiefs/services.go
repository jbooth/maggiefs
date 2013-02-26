package maggiefs

import (

)

// the lease service provides leases (duh), and streams of notifications about file state change
// it's incumbent on client to Acknowledge() each notification in a timely manner or lose leases
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
  // multiple invocations of this method will return multiple channels
  GetNotifier() (chan NotifyEvent, error)
  // sends acknowledgement that we received a notify event
  Acknowledge(NotifyEvent) error
  // blocks until all leases are released for the given node
  WaitAllReleased(nodeid uint64) error
  // re-sets all leases, closes all issued Notifier chans
  RenewLease() error
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
  StatFs() (stat FsStat, err error)
  // persists a new inode to backing store
  AddInode(node Inode) (id uint64, err error)
  // atomically mutates an inode, optimization over WriteLock for small operations
  Mutate(nodeid uint64, mutator func(inode *Inode) error) (newNode *Inode, err error)
  // Links the given child to the given parent, with the given name.  returns error E_EXISTS if force is false and parent already has a child of that name
  Link(parent uint64, child uint64, name string, force bool) (newNode *Inode, err error)
  // Unlinks the child with the given name
  Unlink(parent uint64, name string) (newNode *Inode, err error)
  // add a block to the end of a file, returns new block
  AddBlock(nodeid uint64, length uint32) (newBlock Block, err error)
  // extend a block and the relevant inode
  ExtendBlock(nodeid uint64, blockId uint64, delta uint32) (newBlock Block, err error)
  // truncate a block, shrinking by the amount posited.  shrink to 0 deletes the block
  TruncateBlock(blockId uint64, delta uint32) (err error)
  // called by datanodes to join the cluster, dnId can be negative
  Join(dnId int32, stat DataNodeStat) error
}



type DataService interface {
  // read some bytes
  Read(blk Block, p []byte, pos uint64, length uint64) (err error)

  // start a stateful write
  Write(blk Block) (conn BlockWriter, err error)
}

type BlockWriter interface {
  // return which block id this writer is writing
  BlockId() uint64
  // writes some bytes, extending block if necessary
  Write(p []byte, pos uint64) error
  // flushes changes to system
  Sync() (err error)
  // flushes and closes this writer
  Close() (err error)
}

// interface exposed from datanodes to namenode
// this is typically over a single socket which 
type NameDataIface interface {
  // periodic heartbeat with datanode stats so namenode can keep total stats and re-replicate
  HeartBeat() (*DataNodeStat, error)
  // assigns a volume id to a volume
  Format(volLoc string, volId int32) (*VolumeStat, error)
  // add a block to this datanode/volume
  AddBlock(blk Block, volId int32) error
  // rm block from this datanode/volume
  RmBlock(id uint64, volId int32) error
  // truncate a block
  TruncBlock(blk Block, volId int32, newSize uint32) error
  // get the list of all blocks for a volume
  BlockReport(volId int32) ([]Block,error)
}


