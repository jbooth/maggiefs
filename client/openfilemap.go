package client

import (
  "sync"
  "github.com/jbooth/maggiefs/maggiefs"
)


type OpenFile struct {
  r *Reader
  w *Writer
  lease maggiefs.ReadLease
  writelock maggiefs.WriteLease
}

func (f OpenFile) Close() error {
  var err error = nil
  if (f.r != nil) { err = f.r.Close() }
  if (f.w != nil) { err = f.w.Close() }
  err = f.lease.Release()
  if (f.writelock != nil) { err = f.writelock.Release() }
  return err
}

// ghetto concurrent hashmap
type openFileMap struct {
  numBuckets int
  mapmap map[uint64] openFileMapSlice
}

type openFileMapSlice struct {
  files map[uint64] OpenFile
  lock sync.RWMutex
}

func newOpenFileMap(numBuckets int) openFileMap {
  ret := openFileMap { numBuckets, make(map[uint64] openFileMapSlice) }
  for i := 0 ; i < numBuckets ; i++ {
    ret.mapmap[uint64(i)] = openFileMapSlice{make(map[uint64] OpenFile), sync.RWMutex{}}
  }
  return ret
}

func (m openFileMap) put(k uint64, v OpenFile) {
  slice := m.mapmap[k % uint64(m.numBuckets)]
  slice.lock.Lock()
  defer slice.lock.Unlock()
  slice.files[k] = v
}

func (m openFileMap) get(k uint64) OpenFile {
  slice := m.mapmap[k % uint64(m.numBuckets)]
  slice.lock.RLock()
  defer slice.lock.RUnlock()
  return slice.files[k]
}
