package leaseserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"time"
)

type LeaseClient struct {
	c *rawclient
}

func NewLeaseClient(hostAddr string) (*LeaseClient, error) {
	raw, err := newRawClient(hostAddr)
	if err != nil {
		return nil, err
	}
	return &LeaseClient{raw}, nil
}

// acquires the write lease for the given inode
// only one client may have the writelease at a time, however it is pre-emptable in case r
// a higher priority process (re-replication etc) needs this lease.
// on pre-emption, the supplied commit() function will be called
// pre-emption will not happen while WriteLease.ShortTermLock() is held, however that lock should
// not be held for the duration of anything blocking
func (lc LeaseClient) WriteLease(nodeid uint64) (l maggiefs.WriteLease, err error) {
	req := request{OP_WRITELEASE, 0, nodeid, 0}
	resp, err := lc.c.doRequest(req)
	if err != nil {
		return nil, err
	}
	for resp.Status == STATUS_WAIT {
		time.Sleep(1000 * time.Millisecond)
		resp, err = lc.c.doRequest(req)
		if err != nil {
			return nil, err
		}
	}
	lease := &WriteLease{Lease{resp.Leaseid, nodeid, true, lc.c}}
	return lease, nil
}

// takes out a lease for an inode, this is to keep the posix convention that unlinked files
// aren't cleaned up until they've been closed by all programs
// also registers a callback for when the node is remotely changed, this will be triggered
// upon the file changing *unless* we've cancelled this lease.  Recommend
func (lc LeaseClient) ReadLease(nodeid uint64) (l maggiefs.ReadLease, err error) {
	req := request{OP_READLEASE, 0, nodeid, 0}
	resp, err := lc.c.doRequest(req)
	if err != nil {
		return nil, err
	}
	lease := &Lease{resp.Leaseid, nodeid, false, lc.c}
	return lease, nil
}

// returns a chan which will contain an event every time any inode in the system is changed
// used for cache coherency
// the fuse client runs a goroutine reading all changes from this chan
func (lc LeaseClient) GetNotifier() chan maggiefs.NotifyEvent {
	return lc.c.notifier
}

// blocks until all leases are released for the given node
func (lc LeaseClient) WaitAllReleased(nodeid uint64) error {
	req := request{OP_CHECKLEASES, 0, nodeid, 0}
	resp, err := lc.c.doRequest(req)
	if err != nil {
		return err
	}
	for resp.Status == STATUS_WAIT {
		time.Sleep(1000 * time.Millisecond)
		resp, err = lc.c.doRequest(req)
		if err != nil {
			return err
		}
	}
	return nil
}

type NotifyEvent struct {
	inodeid uint64
	ackid   uint64
	c       *rawclient
}

func (n NotifyEvent) Ack() error {
	// send ack message to server

	req := request{OP_ACKNOWLEDGE, n.ackid, n.inodeid, n.ackid}
	fmt.Printf("Sending ack %+v\n", req)
	n.c.sendRequestNoResponse(req)
	return nil
}

func (n NotifyEvent) Inodeid() uint64 {
	return n.inodeid
}

type Lease struct {
	leaseid    uint64
	inodeid    uint64
	writeLease bool
	c          *rawclient
}

// lets go of lock, committing our changes to all open readleases
func (l *Lease) Release() error {
	var op byte
	if l.writeLease {
		op = OP_WRITELEASE_RELEASE
	} else {
		op = OP_READLEASE_RELEASE
	}
	req := request{op, l.leaseid, l.inodeid, 0}
	_, err := l.c.doRequest(req)
	return err
}

type WriteLease struct {
	Lease
}
