package maggiefs

import ()

// the lease service manages notifications on changes to file data in order to support our use of local page cache
// as well as certain posix conventions regarding delayed delete of unlinked files which are still open by some process
type LeaseService interface {

	// takes out a lease for an inode, this is to keep the posix convention that unlinked files
	// aren't cleaned up until they've been closed by all programs
	// also registers a callback for when the node is remotely changed, this will be triggered
	// upon the file changing *unless* we've cancelled this lease.
	ReadLease(nodeid uint64) (l ReadLease, err error)

	// sends a notification to all holders of ReadLease for this nodeid that its bytes has changed at the
	// offset and length we're advertising here
	// readers must either ack in a timely manner or their lease will be interrupted
	Notify(nodeid uint64, off int64, length int64) (err error)

	// returns a chan which will contain an event every time any inode in the system is changed
	// used for cache coherency
	// the fuse client OpenFileMap runs a goroutine reading all changes from this chan
	GetNotifier() chan NotifyEvent
	// blocks until all leases are released for the given node
	WaitAllReleased(nodeid uint64) error
}

type ReadLease interface {
	// unsubscribes from this ReadLease
	Release() error
}

type NotifyEvent interface {
	// client MUST call ack for every received notifyEvent.  If not, your client lease will expire.
	Ack() error
	// the inode id we're notifying a change for,
	Inodeid() uint64
	// the offset and length of the file that were changed
	OffAndLength() (int64, int64)
}

type NameService interface {
	GetInode(nodeid uint64) (i *Inode, err error)
	StatFs() (stat FsStat, err error)
	// persists a new inode to backing store
	AddInode(node *Inode) (id uint64, err error)
	// Sets attributes on an ino (inode id is part of SetAttrIn)
	SetAttr(nodeid uint64, arg SetAttr) (newNode *Inode, err error)
	SetXAttr(nodeid uint64, name []byte, val []byte) (err error)
	DelXAttr(nodeid uint64, name []byte) (err error)
	// special case for SetLength, allows us to coalesce a couple length updates into a single one when appending
	// will add/remove blocks as necessary, can be used to truncate
	// attempts to place new blocks on requestedDnId if non-nil

	// extends an inode to newLength, no-op if already that long
	// error condition if this would require new blocks that we haven't allocated yet
	Extend(nodeid uint64, newLen uint64) (newNode *Inode, err error)
	// truncates an inode to the provided length, deleting blocks as necessary
	Truncate(nodeid uint64, newLen uint64) (newNode *Inode, err error)
	// adds a new block to the end of this inode if one does not already exist at blockStartPos.
	// does not modify inode.length, you should call Extend after you've written some bytes to the block
	AddBlock(nodeid uint64, blockStartPos uint64, requestedDnId *uint32) (newNode *Inode, err error)
	// fallocates the inode to the given length
	Fallocate(nodeid uint64, length uint64, requestedDnId *uint32) (err error)

	// namespace methods
	// Links the given child to the given parent, with the given name.  returns error E_EXISTS if force is false and parent already has a child of that name
	Link(parent uint64, child uint64, name string, force bool) (err error)
	// Unlinks the child with the given name
	Unlink(parent uint64, name string) (err error)

	// called by datanodes to register the datanode with the cluster
	// nameDataAddr is the address:port that the NN will connect to to administer the DN
	Join(dnId uint32, nameDataAddr string) (err error)
	// called by DNs to obtain a new unique volume id
	NextVolId() (id uint32, err error)
	// called by DNs to obtain a new unique DN id
	NextDnId() (id uint32, err error)
}

type SetAttr struct {
	SetMode  bool
	Mode     uint32
	SetUid   bool
	Uid      uint32
	SetGid   bool
	Gid      uint32
	SetMtime bool
	Mtime    uint64
}
