package leaseserver

import (
  "sync"
  "github.com/4ad/doozer"
)

type readLease struct {
  doozer *doozer.Conn
  leasePath string
  leaseRev int64
}

func (r readLease) Release() error {
  // delete our readpath
  return r.doozer.Del(r.leasePath,r.leaseRev)
}






type writeLease struct {
  doozer *doozer.Conn
  m *sync.Mutex
  leasePath string
  leaseBody []byte
  leaseRev int64
  inodeid uint64
}

func (w writeLease) Release() error {
  w.m.Lock()
  defer w.m.Unlock()
  // delete our write path
  return w.doozer.Del(w.leasePath,w.leaseRev)
}

func (w writeLease) Commit() error {
  w.m.Lock()
  defer w.m.Unlock()
  // modifying node at writepath will notify all readers
  lr,err := w.doozer.Set(w.leasePath,w.leaseRev,w.leaseBody)
  if (err != nil) { return err }
  w.leaseRev = lr
  return nil
}



