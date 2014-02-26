package client

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"sync"
)

// responsible for 4 things:
// 1)  maintaining open FDs and which inode they point to
// 2)  caching said inode so we don't make 2 rpcs per read
// 3)  intercepting notifications from the master and invalidating cache as necessary
// 4)  proving a writeback cache for length changes so that writes are faster
type OpenFileMap struct {
	leases maggiefs.LeaseService
	names  maggiefs.NameService
	datas  maggiefs.DataService
	l      *sync.RWMutex
	files  map[uint64]*openFile
	inodes map[uint64]*openInode
}

// gets our local leased copy of an inode, if possible, or nil if we have no copy of that ino open
func (o *OpenFileMap) GetInode(id uint64) (*maggiefs.Inode, error) {
	o.l.Rlock()
	openIno, exists := o.inodes[id]
	o.l.RUnlock()
	if !exists {
		return nil, nil
	}
	var ret *maggiefs.Inode
	openIno.l.RLock()
	ret := openIno.ino
	openIno.l.RUnlock()
	// if valid, return it
	if ret != nil {
		return ret, nil
	}
	// could have been invalidated, in which case we should re-acquire a copy
	openIno.l.Lock()
	defer openIno.l.Unlock()
	ino, err := o.names.GetInode(id)
	if err != nil {
		return nil, err
	}
	openIno.ino = ino
	return ino, nil
}

func (o *OpenFileMap) SetLength(inodeid uint64, newLen uint64) error {
	o.l.RLock()

}

func (o *OpenFileMap) getInodeForFd(fd uint64) (*maggiefs.Inode, error) {

}

type openInode struct {
	names       maggiefs.NameService
	l           *sync.RWMutex
	ino         *maggiefs.Inode
	lease       maggiefs.ReadLease
	refCount    int
	dirtyLength bool // we don't always update master on length change, specifically
	newLength   uint64
}

// flushes all changes to master
func (i *openInode) Sync() {

}

// sets length, might only set it locally and defer flush to master
func (i *openInode) SetLength(uint64 newLen) {

}

type openFile struct {
	fh    uint64
	ino   *openInode
	w     *InodeWriter
	lease maggiefs.ReadLease
}

func (f *openFile) Read() {

}

func (f *openFile) Write() {

}

func (f *openFile) Fsync() {

}
func (f *openFile) Close() error {
	var err error = nil
	if f.r != nil {
		err = f.r.Close()
	}
	if f.w != nil {
		err = f.w.Fsync()
		if err != nil {
			return err
		}
		err = f.w.Close()
		if err != nil {
			return err
		}
	}
	err = f.lease.Release()
	return err
}

func (f *openFile) String() string {
	return fmt.Sprintf("openFile{Fh: %d inode %d}", f.fh, f.inodeid)
}

// ghetto concurrent hashmap
type openFileMap struct {
	numBuckets int
	mapmap     map[uint64]openFileMapSlice
}

type openFileMapSlice struct {
	files map[uint64]*openFile
	lock  sync.RWMutex
}

func newOpenFileMap(numBuckets int) openFileMap {
	ret := openFileMap{numBuckets, make(map[uint64]openFileMapSlice)}
	for i := 0; i < numBuckets; i++ {
		ret.mapmap[uint64(i)] = openFileMapSlice{make(map[uint64]*openFile), sync.RWMutex{}}
	}
	return ret
}

func (m openFileMap) put(k uint64, v *openFile) {
	slice := m.mapmap[k%uint64(m.numBuckets)]
	slice.lock.Lock()
	defer slice.lock.Unlock()
	slice.files[k] = v
}

func (m openFileMap) get(k uint64) *openFile {
	slice := m.mapmap[k%uint64(m.numBuckets)]
	slice.lock.RLock()
	defer slice.lock.RUnlock()
	return slice.files[k]
}
