package client

import (
	"fmt"
	"github.com/jbooth/maggiefs/fuse"
	"github.com/jbooth/maggiefs/maggiefs"
	"log"
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
	onNotify  func(uint64)
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
	w   *Writer
}

func NewOpenFileMap(leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService, localDnId *uint32, onNotify func(uint64)) *OpenFileMap {
	ret := &OpenFileMap{
		leases,
		names,
		datas,
		localDnId,
		0,
		new(sync.RWMutex),
		make(map[uint64]*openFile),
		make(map[uint64]*openInode),
		onNotify,
	}
	go ret.handleNotifications()
	return ret
}

func (o *OpenFileMap) handleNotifications() {
	notifications := o.leases.GetNotifier()
	for event := range notifications {
		inodeid := event.Inodeid()
		o.clear(inodeid)
		o.onNotify(inodeid)
		err := event.Ack()
		if err != nil {
			log.Printf("Error acking on notify for ino %d : %s", inodeid, err)
		}
	}
}

// all clients should call leaseservice.Notify through here, so our local copies
// of inodes associated with open files are updated properly
// does NOT call our local onNotify method (we don't want to mess with page locks and possibly conflict with writes)
func (o *OpenFileMap) doNotify(inodeid uint64, off int64, length int64) (err error) {
	if off > 0 || length > 0 {
		err = o.leases.Notify(inodeid, off, length)
	}
	o.clear(inodeid)
	return err
}

func (o *OpenFileMap) clear(inodeid uint64) {
	o.l.RLock()
	openIno := o.inodes[inodeid]
	o.l.RUnlock()
	openIno.l.Lock()
	openIno.ino = nil
	openIno.l.Unlock()
}

func (o *OpenFileMap) Open(inodeid uint64, writable bool) (fh uint64, err error) {
	o.l.Lock()
	defer o.l.Unlock()
	o.fhCtr = o.fhCtr + 1
	fh = o.fhCtr
	var w *Writer
	if writable {
		w = NewWriter(inodeid, o.doNotify, o.names, o.datas, o.localDnId)
	}
	// check inode
	ino, exists := o.inodes[inodeid]
	if !exists {
		// this ino is not open anywhere else, get a new lease
		lease, err := o.leases.ReadLease(inodeid)
		if err != nil {
			return 0, err
		}
		ino = &openInode{
			new(sync.RWMutex),
			inodeid,
			nil,
			lease,
			1,
		}
		o.inodes[inodeid] = ino
	} else {
		// incr refcount for existing lease
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
	ino := f.ino
	ino.l.Lock()
	ino.refCount--
	if ino.refCount == 0 {
		delete(o.inodes, ino.inodeid)
		ino.lease.Release()
	}
	ino.l.Unlock()
	o.l.Unlock()
	if f.w != nil {
		return f.w.Close()
	}
	return nil
}

func (o *OpenFileMap) Read(fd uint64, buf fuse.ReadPipe, pos uint64, length uint32) (err error) {
	_, ino, err := o.getInode(fd)
	if err != nil {
		return err
	}
	// try 3 times before giving up
	// DataService marks nodes as bad if we fail and will reroute to another on the next attempt
	for i := 0; i < 3; i++ {
		err = Read(o.datas, ino, buf, pos, length)
		if err == nil {
			return nil
		}
	}
	return err
}

func (o *OpenFileMap) Write(fd uint64, p []byte, pos uint64, length uint32) (nWritten uint32, err error) {

	f, ino, err := o.getInode(fd)
	if err != nil {
		return 0, err
	}
	return length, f.w.Write(o.datas, ino, p, pos, length)
}

func (o *OpenFileMap) Sync(fd uint64) (err error) {

	f, _, err := o.getInode(fd)
	if err != nil {
		return err
	}
	if f.w != nil {
		return f.w.Sync()
	}
	return nil
}

// gets our local leased copy of an inode, if possible, or nil if we have no copy of that ino open
func (o *OpenFileMap) getInode(fd uint64) (*openFile, *maggiefs.Inode, error) {
	o.l.RLock()
	f, exists := o.files[fd]
	o.l.RUnlock()
	if !exists {
		return nil, nil, nil
	}
	openIno := f.ino
	var ret *maggiefs.Inode
	openIno.l.RLock()
	ret = openIno.ino
	openIno.l.RUnlock()
	// if valid, return it
	if ret != nil {
		return f, ret, nil
	}
	// could have been invalidated, in which case we should re-acquire a copy
	openIno.l.Lock()
	defer openIno.l.Unlock()
	ino, err := o.names.GetInode(openIno.inodeid)
	if err != nil {
		return nil, nil, err
	}
	openIno.ino = ino
	return f, ino, nil
}
