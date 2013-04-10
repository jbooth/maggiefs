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

type NotifyEvent struct {
  TxnId  uint64
  NodeId uint64
  Time   int64
}

type NameService interface {
  GetInode(nodeid uint64) (i *Inode, err error)
  StatFs() (stat FsStat, err error)
  // persists a new inode to backing store
  AddInode(node *Inode) (id uint64, err error)
  // sets an existing inode, write lease should be held for this
  SetInode(node *Inode) (err error)
  // truncate an inode to the given length, deleting blocks if necessary, write lease should be held for this
  Truncate(nodeid uint64, newSize uint64) (err error)
  // Links the given child to the given parent, with the given name.  returns error E_EXISTS if force is false and parent already has a child of that name
  Link(parent uint64, child uint64, name string, force bool) (err error)
  // Unlinks the child with the given name
  Unlink(parent uint64, name string) (err error)
  // add a block to the end of a file, returns new block
  AddBlock(nodeid uint64, length uint32) (newBlock Block, err error)
  // called by datanodes to register the datanode with the cluster
  // nameDataAddr is the address:port that the NN will connect to to administer the DN
  Join(dnId int32, nameDataAddr string) (err error)
  // called by DNs to obtain a new unique volume id
  NextVolId() (id int32, err error)
  // called by DNs to obtain a new unique DN id
  NextDnId() (id int32, err error)
}