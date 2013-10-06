package integration

import (
	"fmt"
	"sync"
	"github.com/jbooth/go-fuse/fuse"
)

type Mount struct {
	Ms         *fuse.MountState
	MountPoint string
	closed bool
	closeCnd *sync.Cond
}

func newMountedClient(mfs fuse.RawFileSystem, mountPoint string, debug bool) (*Mount, error) {
	mountState := fuse.NewMountState(mfs)

	mountState.Debug = debug
	opts := &fuse.MountOptions{
		MaxBackground: 12,
		//Options: []string {"ac_attr_timeout=0"},//,"attr_timeout=0","entry_timeout=0"},
	}
	err := mountState.Mount(mountPoint, opts)
	return &Mount{mountState, mountPoint, false, new(sync.Cond)}, err
}

func (m *Mount) Serve() error {
	// catch panics and turn into channel send
	defer func() {
		if x := recover(); x != nil {
			fmt.Printf("run time panic serving mountpoint %s : %v\n", m.MountPoint, x)
		}
	}()
	m.Ms.Loop()
	return nil
}

func (m *Mount) Close() error {
	defer func() {
		if x := recover(); x != nil {
			fmt.Printf("run time panic: %v\n", x)
		}
	}()
	err := m.Ms.Unmount()
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


