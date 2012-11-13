package leaseserver

import (
  "github.com/4ad/doozer"
)

type ReadLease struct {
  doozer *doozer.Conn
  leasePath string
  leaseRev int64
  inodeid uint64
  releaseChan chan int
}

func (r ReadLease) Release() error {
  // unregister our channel with the
  r.releaseChan <- 1 
  // delete our readpath
  return r.doozer.Del(r.leasePath,r.leaseRev)
}






type WriteLease struct {
  doozer *doozer.Conn
  leaseRoot string
  leasePath string
  leaseRev int64
  inodeid uint64
}

func (w WriteLease) Release() error {
  // delete our write path
  return w.doozer.Del(w.leasePath,w.leaseRev)
}

func (w WriteLease) Commit() error {
  // push notification to all readers
  return nil
}



