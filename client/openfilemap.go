package client

import (
	"fmt"
	"github.com/jbooth/maggiefs/fuse"
	"github.com/jbooth/maggiefs/maggiefs"
	"sync"
)

// responsible for 3 things:
// 1)  maintaining open FDs and which inode they point to
// 2)  caching said inode so we don't make 2 rpcs per read
// 3)  intercepting notifications from the master and invalidating cache as necessary
type OpenFileMap struct {
	leases    maggiefs.LeaseService
	names     maggiefs.NameService
	datas     maggiefs.DataService
	localDnId *uint32
	fhCtr     uint64
	l         *sync.RWMutex
	files     map[uint64]*openFile
	inodes    map[uint64]*openInode
}

type openInode struct {
	l        *sync.RWMutex
	inodeid  uint64
	ino      *maggiefs.Inode
	lease    maggiefs.ReadLease
	refCount int
}

type openFile struct {
	fh  uint64
	ino *openInode
	w   *InodeWriter
}

func (o *OpenFileMap) Open(inodeid uint64, writable bool) (fh uint64, err error) {
	o.l.Lock()
	defer o.l.Unlock()
	o.fhCtr = o.fhCtr + 1
	fh := o.fhCtr
	var w *InodeWriter
	if writable {
		w = NewInodeWriter(inodeid, o.leases, o.names, o.datas)
	}
	// check inode
	ino, exists := o.inodes[inodeid]
	if !exists {
		lease, err := o.leases.ReadLease(inodeid)
		if err != nil {
			return 0, err
		}
		ino = &openInode{
			new(sync.Mutex),
			inodeid,
			nil,
			lease,
			1,
		}
		o.inodes[inodeid] = ino
	} else {
		// incr refcount
		ino.l.Lock()
		ino.refCount++
		ino.l.Unlock()
	}
	f := &openFile{
		fh,
		ino,
		w,
	}
	o.files[f.fh] = f
	return fh, nil
}

func (o *OpenFileMap) Close(fh uint64) (err error) {
	o.l.Lock()
	f, exists := o.files[fh]
	if !exists {
		o.l.Unlock()
		return fmt.Errorf("No file with fh %d", fh)
	}
	delete(o.files, fh)
	ino, exists := o.inodes[f.inodeid]
	if !exists {
		// this should be impossible
		o.l.Unlock()
		return fmt.Errorf("How on earth do we not have inode %d for fh %d", inodeid, fh)
	}
	ino.l.Lock()
	ino.refCount--
	if ino.refCount == 0 {
		delete(o.inodes, f.inodeid)
		ino.lease.Release()
	}
	ino.l.Unlock()
	o.l.Unlock()
	return f.w.Close()
}

func (o *OpenFileMap) Read(fd uint64, buf fuse.ReadPipe, pos uint64, length uint32) (err error) {
	_, ino, err := o.getInode(fd)
	if err != nil {
		return err
	}
	return doRead(o.datas, ino, buf, pos, length)
	//doRead(datas maggiefs.DataService, inode *maggiefs.Inode, p fuse.ReadPipe, position uint64, length uint32)
}

func (o *OpenFileMap) Write(fd uint64, p []byte, pos uint64, length uint32) (err error) {

	f, ino, err := o.getInode(fd)
	if err != nil {
		return err
	}
	return f.w.doWrite(datas, ino, p, pos, length)
	//Write(datas maggiefs.DataService, inode *maggiefs.Inode, p []byte, position uint64, length uint32)
}

// gets our local leased copy of an inode, if possible, or nil if we have no copy of that ino open
func (o *OpenFileMap) getInode(fd uint64) (*openFile, *maggiefs.Inode, error) {
	o.l.Rlock()
	f, exists := o.files[id]
	o.l.RUnlock()
	if !exists {
		return nil, nil, nil
	}
	openIno = f.ino
	var ret *maggiefs.Inode
	openIno.l.RLock()
	ret := openIno.ino
	openIno.l.RUnlock()
	// if valid, return it
	if ret != nil {
		return f, ret, nil
	}
	// could have been invalidated, in which case we should re-acquire a copy
	openIno.l.Lock()
	defer openIno.l.Unlock()
	ino, err := o.names.GetInode(id)
	if err != nil {
		return nil, err
	}
	openIno.ino = ino
	return f, ino, nil
}
