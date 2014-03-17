package leaseserver

import (
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

// sends notification
func (lc LeaseClient) Notify(nodeid uint64, off int64, length int64) (err error) {
	req := request{OP_NOTIFY, 0, nodeid, 0, off, length}
	_, err = lc.c.doRequest(req)
	return err
}

// takes out a lease for an inode, this is to keep the posix convention that unlinked files
// aren't cleaned up until they've been closed by all programs
// also registers a callback for when the node is remotely changed, this will be triggered
// upon the file changing *unless* we've cancelled this lease.  Recommend
func (lc LeaseClient) ReadLease(nodeid uint64) (l maggiefs.ReadLease, err error) {
	req := request{OP_READLEASE, 0, nodeid, 0, 0, 0}
	resp, err := lc.c.doRequest(req)
	if err != nil {
		return nil, err
	}
	lease := &Lease{resp.Leaseid, nodeid, lc.c}
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
	req := request{OP_CHECKLEASES, 0, nodeid, 0, 0, 0}
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
	ackid   uint64
	inodeid uint64
	offset  int64
	length  int64
	c       *rawclient
}

func (n NotifyEvent) Ack() error {
	// send ack message to server

	req := request{OP_ACKNOWLEDGE, n.ackid, n.inodeid, n.ackid, 0, 0}
	n.c.sendRequestNoResponse(req)
	return nil
}

func (n NotifyEvent) Inodeid() uint64 {
	return n.inodeid
}

func (n NotifyEvent) OffAndLength() (int64, int64) {
	return n.offset, n.length
}

type Lease struct {
	leaseid uint64
	inodeid uint64
	c       *rawclient
}

// lets go of lock, committing our changes to all open readleases
func (l *Lease) Release() error {
	var op = OP_READLEASE_RELEASE
	req := request{op, l.leaseid, l.inodeid, 0, 0, 0}
	_, err := l.c.doRequest(req)
	return err
}
