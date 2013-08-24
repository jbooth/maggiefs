package client

import (
  "github.com/jbooth/maggiefs/maggiefs"
  "sync"
  "time"
)

// wrapper for nameservice and leaseservice that caches inodes

type NameCache struct {
  names maggiefs.NameService
  leases maggiefs.LeaseService
  numBuckets int  // number of hash buckets we use
  maxPerBucket int  // high water mark for each bucket -- when we hit this, we clean up until at minPerBucket
  minPerBucket int
  notifier chan uint64
  // cache
  stripelock map[uint64] *sync.Mutex
  mapmap map[uint64] map[uint64] *centry  
}

func NewNameCache(names maggiefs.NameService, leases maggiefs.LeaseService) *NameCache {
	nc := &NameCache {
		names,
		leases,
		10,
		512,
		256,
		make(chan uint64),
		make(map[uint64]*sync.Mutex),
		make(map[uint64] map[uint64]*centry),
	}
	for i := uint64(0) ; i < 10 ; i++ {
		nc.stripelock[i] = new(sync.Mutex)
		nc.mapmap[i] = make(map[uint64]*centry)
	}
	return nc
}

type centry struct {
  i *maggiefs.Inode
  rl maggiefs.ReadLease
  lastUsed int64
}

func (nc *NameCache) withLock(nodeid uint64, f func (m map[uint64] *centry)) {
  mod := nodeid % uint64(nc.numBuckets)
  l := nc.stripelock[mod]
  l.Lock()
  defer l.Unlock()
  f(nc.mapmap[mod])
}

// name cache methods
func (nc *NameCache) invalidate(nodeid uint64) {
  nc.withLock(nodeid, func(m map[uint64] *centry) {
    c := m[nodeid]
    c.rl.Release()
    delete(m,nodeid)
  })
}

func (nc *NameCache) getIfCached(nodeid uint64) (i *maggiefs.Inode) {
  var ret *maggiefs.Inode = nil
  nc.withLock(nodeid, func(m map[uint64] *centry) {
    c,exists := m[nodeid]
    if exists {
      ret = c.i
      c.lastUsed = time.Now().Unix()
    }
  })
  return ret
}

func (nc *NameCache) setInCache(i *maggiefs.Inode) error {
  var e error = nil
  nc.withLock(i.Inodeid, func(m map[uint64] *centry) {
    c,exist := m[i.Inodeid]
    if (exist) {
      c.i = i
    } else {
      rl,err := nc.leases.ReadLease(i.Inodeid)
      if err != nil {
        e = err
        return
      }
      m[i.Inodeid] = &centry{i,rl,time.Now().Unix()}    
    }
  })
  return e
}



// see maggiefs/nameservice.go for docs on these interface methods

func (nc *NameCache) WriteLease(nodeid uint64) (l maggiefs.WriteLease, err error) {
  return nc.leases.WriteLease(nodeid)
}

func (nc *NameCache) ReadLease(nodeid uint64) (l maggiefs.ReadLease, err error) {
  return nc.leases.ReadLease(nodeid)
}

func (nc *NameCache) GetNotifier() (chan uint64) {
  return nc.notifier
}

func (nc *NameCache) WaitAllReleased(nodeid uint64) error {
  return nc.leases.WaitAllReleased(nodeid)
}


// name service methods

func (nc *NameCache)   GetInode(nodeid uint64) (i *maggiefs.Inode, err error) {
  return nil,nil
}

func (nc *NameCache)   StatFs() (stat maggiefs.FsStat, err error) {
  return nc.names.StatFs()
}

  // persists a new inode to backing store
func (nc *NameCache)   AddInode(node *maggiefs.Inode) (id uint64, err error) {
  return nc.names.AddInode(node)
}
  // sets an existing inode, write lease should be held for this
func (nc *NameCache)   SetInode(node *maggiefs.Inode) (err error) {
  err = nc.names.SetInode(node)
  if err != nil {
    nc.invalidate(node.Inodeid)
    return err
  }
  err = nc.setInCache(node)
  return err
}

  // truncate an inode to the given length, deleting blocks if necessary, write lease should be held for this
func (nc *NameCache)   Truncate(nodeid uint64, newSize uint64) (err error) {
  err = nc.names.Truncate(nodeid,newSize)
  nc.invalidate(nodeid)
  return err
}
  // Links the given child to the given parent, with the given name.  returns error E_EXISTS if force is false and parent already has a child of that name
func (nc *NameCache)   Link(parent uint64, child uint64, name string, force bool) (err error) {
  err = nc.names.Link(parent,child,name,force)
  nc.invalidate(parent)
  nc.invalidate(child) // not sure if this actually necessary but hey
  return err
}
  // Unlinks the child with the given name
func (nc *NameCache)   Unlink(parent uint64, name string) (err error) {
  err = nc.names.Unlink(parent,name)
  nc.invalidate(parent)
  return err
}

  // add a block attached to this inode, returns new block
func (nc *NameCache)   AddBlock(nodeid uint64, length uint32) (newBlock maggiefs.Block, err error) {
  block,err := nc.names.AddBlock(nodeid,length)
  nc.invalidate(nodeid)
  return block,err
}



  // called by datanodes to register the datanode with the cluster
  // nameDataAddr is the address:port that the NN will connect to to administer the DN
func (nc *NameCache)   Join(dnId uint32, nameDataAddr string) (err error) {
  return nc.names.Join(dnId,nameDataAddr)
}
  // called by DNs to obtain a new unique volume id
func (nc *NameCache)   NextVolId() (id uint32, err error) {
  return nc.names.NextVolId()
}
  // called by DNs to obtain a new unique DN id
func (nc *NameCache)   NextDnId() (id uint32, err error) {
  return nc.names.NextDnId()
}