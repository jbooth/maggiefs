package maggiefs

import (
	"sync"
)

type singleWriteLease struct {
	i *inodeLeaseState
	notifier chan uint64
}

func (s *singleWriteLease) Release() error {
	s.i.Lock()
	defer s.i.Unlock()
	s.i.outstandingWriteLease = nil
	// signal to any that were waiting on write lease becoming available
	s.i.writeLeaseClosed.Broadcast()
	return nil
}

func (s *singleWriteLease) Commit() error {
	// notify the notifier
	s.notifier <- s.i.inodeId
	return nil
}

type singleReadLease struct {
	i        *inodeLeaseState
	myId     uint64
	changeNotify chan uint64
}

func (s *singleReadLease) Release() error {
	s.i.Lock()
	defer s.i.Unlock()
	delete(s.i.outstandingReadLeases, s.myId)
	if len(s.i.outstandingReadLeases) == 0 {
		s.i.allReadLeasesClosed.Broadcast()
	}
	return nil
}

// represents lease state for a single inode
// tracks all outstanding read leases and provides callbacks for them to deregister themselves
type inodeLeaseState struct {
	*sync.Mutex
	inodeId               uint64
	idCounter             uint64
	outstandingReadLeases map[uint64]*singleReadLease
	outstandingWriteLease *singleWriteLease
	allReadLeasesClosed   *sync.Cond // signalled when readLeases == 0 after closing
	writeLeaseClosed      *sync.Cond
}

func (s *inodeLeaseState) ReadLease() ReadLease {
	s.Lock()
	defer s.Unlock()
	newId := IncrementAndGet(&s.idCounter, 1)
	rl := &singleReadLease{
		i:        s,
		myId:     newId}
	s.outstandingReadLeases[newId] = rl
	return rl
}

func (s *inodeLeaseState) WriteLease(notifier chan uint64) WriteLease {
	// need to atomically make sure we can get writelease
	s.Lock()
	defer s.Unlock()
	// wait until writeLease == nil
	for s.outstandingWriteLease != nil {
		s.writeLeaseClosed.Wait()
	}
	s.outstandingWriteLease = &singleWriteLease{s,notifier}
	return s.outstandingWriteLease
}

func newInodeLeaseState(inode uint64) *inodeLeaseState {
	l := new(sync.Mutex)
	return &inodeLeaseState{
		l,
		inode,
		uint64(0),
		make(map[uint64]*singleReadLease),
		nil,
		sync.NewCond(l),
		sync.NewCond(l),
	}

}

// on writer commit, readers need to be notified
// on writer takeover, writer needs to be removed (not needed yet)
type LocalLeases struct {
	numBuckets uint64
	maps       map[uint64]submap
	notifier chan uint64
}

type submap struct {
	l *sync.Mutex
	m map[uint64]*inodeLeaseState
}

func NewLocalLeases() LocalLeases {
	numBuckets := 10
	ret := LocalLeases{uint64(numBuckets), make(map[uint64]submap),make(chan uint64,100)}
	for i := 0; i < numBuckets; i++ {
		ret.maps[uint64(i)] = submap{new(sync.Mutex), make(map[uint64]*inodeLeaseState)}
	}
	return ret
}

func (l LocalLeases) getOrCreateInodeLeaseState(nodeid uint64) *inodeLeaseState {
	idx := nodeid % l.numBuckets
	m := l.maps[idx]
	m.l.Lock()
	defer m.l.Unlock()
	prev, exists := m.m[nodeid]
	if exists {
		return prev
	}
	// else
	prev = newInodeLeaseState(nodeid)
	m.m[nodeid] = prev
	return prev
}

func (l LocalLeases) WriteLease(nodeid uint64) (lease WriteLease, err error) {
  inodeLease := l.getOrCreateInodeLeaseState(nodeid)
  return inodeLease.WriteLease(l.notifier),nil
}

func (l LocalLeases) ReadLease(nodeid uint64) (lease ReadLease, err error) {
  return l.getOrCreateInodeLeaseState(nodeid).ReadLease(),nil
}

func (l LocalLeases) GetNotifier() chan uint64 {
  return l.notifier
}

func (l LocalLeases) WaitAllReleased(nodeid uint64) error {
  inodeLease := l.getOrCreateInodeLeaseState(nodeid)
  inodeLease.Lock()
  defer inodeLease.Unlock()
  inodeLease.allReadLeasesClosed.Wait()
	return nil
}

//
//type LeaseService interface {
//  
//  // acquires the write lease for the given inode
//  // only one client may have the writelease at a time, however it is pre-emptable in case 
//  // a higher priority process (re-replication etc) needs this lease.
//  // on pre-emption, the supplied commit() function will be called
//  // pre-emption will not happen while WriteLease.ShortTermLock() is held, however that lock should 
//  // not be held for the duration of anything blocking
//  WriteLease(nodeid uint64, commit func(), onChange func(*Inode)) (l WriteLease, err error)
//  
//  // takes out a lease for an inode, this is to keep the posix convention that unlinked files
//  // aren't cleaned up until they've been closed by all programs
//  // also registers a callback for when the node is remotely changed, so we can signal to page cache on localhost
//  ReadLease(nodeid uint64, onChange func(*Inode)) (l Lease, err error)
//  
//  // returns number of outstanding leases for the given node so we can know whether to garbage collect or not
//  NumOutstandingLeases(nodeid uint64) (n int, err error)
//}
//
//type WriteLease interface {
//  Lease
//  // commits changes, notifies readers afterwards.  May yield and reacquire lock.
//  Commit() 
//  // fetch a short term lock to insure that we're not interrupted by a write takeover during a short-lived op
//  // write takeovers can happen once this lock is released
//  ShortTermLock() *sync.Mutex
//}
//
//type Lease interface {
//  Release() error
//}
