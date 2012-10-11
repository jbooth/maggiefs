package maggiefs

import (
  "syscall"
  "sync"
  "sync/atomic"
  "unsafe"
  "errors"
  "time"
)
  //GetInode(nodeid uint64) (i *Inode, err error)
  //StatFs() (statfs syscall.Statfs_t, err error)
  // persists a new inode to backing store
  //AddInode(node Inode) (id uint64, err error)
  //// acquires write lock
  //WriteLock(nodeid uint64) (lock WriteLock, err error)
  //// atomically mutates an inode, optimization over WriteLock for small operations
  //Mutate(nodeid uint64, mutator func(inode *Inode) error) (newNode *Inode, err error)
  //// add a block to the end of a file, returns new block
  //AddBlock(nodeid uint64) (newBlock Block, err error)
  //// takes out a lease for an inode, this is to keep the posix convention that unlinked files
  //// aren't cleaned up until they've been closed by all programs
  //Lease(nodeid uint64) (ls Lease, err error)


// implements nameservice
type memnode struct {
  node *Inode
  wlock *sync.Mutex
  mlock *sync.Mutex // guards for mutations, could be replaced with lockless algo
}
type MemNames struct {
  dataNode NameDataIface
  nodes map[uint64] *memnode
  inodeIdCounter uint64
  blockIdCounter uint64
  l *sync.RWMutex
}

func NewMemNames(dataNode NameDataIface) *MemNames {
  return &MemNames{dataNode, make(map[uint64] *memnode), uint64(0), uint64(0), new(sync.RWMutex)}
}

type wlwrapper struct {
  wlock *sync.Mutex
}

func (w wlwrapper) Unlock() error {
  w.wlock.Unlock()
  return nil
}

//func NewMemNames() (names NameService) {
  //return MemNames{make(map[string]PathEntry),make(map[uint64]Inode),uint64(0),sync.Mutex{}}
//}

func (n MemNames) GetInode(nodeid uint64) (i *Inode, err error) {
  n.l.RLock()
  defer n.l.RUnlock()
  return n.nodes[nodeid].node,nil
}

func (n MemNames) AddInode(node Inode) (id uint64, err error) {
  n.l.Lock()
  defer n.l.Unlock()
  id = atomic.AddUint64(&n.inodeIdCounter,uint64(1))
  node.Inodeid = id
  n.nodes[id] = &memnode{ &node, new(sync.Mutex), new(sync.Mutex) }
  return id,nil
}

func (n MemNames) StatFs() (stat syscall.Statfs_t, err error) {
  n.l.RLock()
  defer n.l.RUnlock()
  return syscall.Statfs_t{},nil
}

func (n MemNames) WriteLock(nodeid uint64) (w WriteLock,err error) {
  n.l.RLock()
  defer n.l.RUnlock()
  return wlwrapper{ n.nodes[nodeid].wlock },nil
}

func (n MemNames) Mutate(nodeid uint64, mutator func(inode *Inode) error) (newNode *Inode, err error) {
  n.l.RLock()
  defer n.l.RUnlock()
  mn := n.nodes[nodeid]
  mn.mlock.Lock()
  defer mn.mlock.Unlock()
  newNode = CopyInode(mn.node)
  err = mutator(newNode)
  if (err != nil) { return nil,err }
  atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&mn.node)),unsafe.Pointer(mn.node))

  // TODO handle case where Nlinks becomes 0 -- or don't i guess, whatever, we'll just orphan them
  return mn.node,nil
}

func (n MemNames) AddBlock(nodeid uint64) (newBlock Block, err error) {
  newId := atomic.AddUint64(&n.blockIdCounter,uint64(1))
  node,err := n.Mutate(nodeid, func(inode *Inode) error {
    lastBlock := inode.Blocks[len(inode.Blocks) - 1]
    // sanity check that last block is actually full
    if (lastBlock.EndPos - lastBlock.StartPos < BLOCKLENGTH) { 
      return errors.New("Last block not quite full!") 
    }
    newBlock := Block {
      Id: newId,
      Mtime: uint64(time.Now().Unix()),
      Inodeid: inode.Inodeid,
      StartPos: lastBlock.EndPos + uint64(1),
      EndPos: lastBlock.EndPos + uint64(1), // size initially 0
      Locations: make([]string,0,0),
    }
    inode.Blocks = append(inode.Blocks,newBlock)
    return nil
  })
  if (err != nil) { 
    return Block{},err 
  } 
  // block added, now make sure datanode is cool
  n.dataNode.AddBlock(node.Blocks[len(node.Blocks)-1].Id)
  return node.Blocks[len(node.Blocks) - 1],nil
}

func (n MemNames) Lease(nodeid uint64) (ls Lease, err error) { 
  return &nonLease{},nil
}
type nonLease struct {
}

func (n nonLease) Release() error {
  return nil
}


