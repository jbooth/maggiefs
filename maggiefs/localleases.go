package maggiefs

import (

  "sync"
)

type onelease struct {
  releaseChan chan bool
  released bool
  onChangeChan chan bool
}

func (l onelease) Release() {
  l.releaseChan <- true
}

type LocalLeases struct {

  bigLock *sync.Mutex
  lmap map[uint64] onelease
}

func NewLocalLeases() LeaseService {
  return LocalLeases{new(sync.Mutex),map[uint64]onelease{}}
}

func (l LocalLeases) WriteLease(nodeid uint64, commit func(), onChange func(*Inode)) (lease WriteLease, err error) {
  return nil,nil
}

func (l LocalLeases) ReadLease(nodeid uint64, onChange func(*Inode)) (lease Lease, err error) {
  return nil,nil
}

func (l LocalLeases) WaitAllReleased(nodeid uint64) error {
  return nil
}
//
//type LeaseService interface {
//  
//  // acquires the write lease for the given inode
//  // only one client may have the writelease at a time, however it is pre-emptable in case 
//  // a higher priority process (re-replication etc) needs this lease.
//  // on pre-emption, the supplied commit() function will be called
//  // pre-emption will not happen while WriteLease.ShortTermLock() is held, however that lock should 
//  // not be held for the duration of anything blocking
//  WriteLease(nodeid uint64, commit func(), onChange func(*Inode)) (l WriteLease, err error)
//  
//  // takes out a lease for an inode, this is to keep the posix convention that unlinked files
//  // aren't cleaned up until they've been closed by all programs
//  // also registers a callback for when the node is remotely changed, so we can signal to page cache on localhost
//  ReadLease(nodeid uint64, onChange func(*Inode)) (l Lease, err error)
//  
//  // returns number of outstanding leases for the given node so we can know whether to garbage collect or not
//  NumOutstandingLeases(nodeid uint64) (n int, err error)
//}
//
//type WriteLease interface {
//  Lease
//  // commits changes, notifies readers afterwards.  May yield and reacquire lock.
//  Commit() 
//  // fetch a short term lock to insure that we're not interrupted by a write takeover during a short-lived op
//  // write takeovers can happen once this lock is released
//  ShortTermLock() *sync.Mutex
//}
//
//type Lease interface {
//  Release() error
//}
