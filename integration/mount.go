package integration

import (
	"fmt"
	"sync"
	"github.com/jbooth/maggiefs/fuse"
)

type Mount struct {
	Mnt         *fuse.Server
	MountPoint string
	closed bool
	closeCnd *sync.Cond
}

func NewMount(mfs fuse.RawFileSystem, mountPoint string, debug bool) (*Mount, error) {
	opts := &fuse.MountOptions{
		MaxBackground: 12,
		MaxWrite: 128 * 1024,
	}
	mnt,err := fuse.NewServer(mfs,mountPoint,opts)
	if err != nil {
		return nil,err
	}
	mnt.SetDebug(debug)
	return &Mount{mnt, mountPoint, false, sync.NewCond(new(sync.Mutex))}, err
}

func (m *Mount) Serve() error {
	// catch panics and turn into channel send
	defer func() {
		if x := recover(); x != nil {
			fmt.Printf("run time panic serving mountpoint %s : %v\n", m.MountPoint, x)
			m.Close()
		}
	}()
	m.Mnt.Serve()
	return nil
}

func (m *Mount) Close() error {
	fmt.Printf("Closing mountpoint at %s\n",m.MountPoint)
	defer func() {
		if x := recover(); x != nil {
			fmt.Printf("run time panic: %v\n", x)
		}
	}()
	err := m.Mnt.Unmount()
	for err != nil {
		err = m.Mnt.Unmount()
	}
	m.closed = true
	m.closeCnd.Broadcast()
	return err
}

func (m *Mount) WaitClosed() error {
	m.closeCnd.L.Lock()
	for !m.closed {
		m.closeCnd.Wait()
	}
	m.closeCnd.L.Unlock()
	return nil
}


