package maggiefs

import (
  "syscall"
  "sync"
  "sync/atomic"
  "errors"
  "time"
  "fmt"
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
  mlock *sync.Mutex // guards for mutations, could be replaced with lockless algo
}
type MemNames struct {
  dataNode DataService
  nodes map[uint64] *memnode
  inodeIdCounter uint64
  blockIdCounter uint64
  l *sync.RWMutex
}

func emptyDir(id uint64) *Inode {
  currTime := time.Now().Unix()
  return &Inode{
    id, 
    0,
    FTYPE_DIR,
    0,
    0777,
    currTime,
    currTime,
    1,
    0,
    0,
    "",
    make([]Block,0,0),
    make(map[string] Dentry),
    make(map[string] []byte),
    }
}

func NewMemNames(dataNode DataService) *MemNames {
  ret := &MemNames{dataNode, make(map[uint64] *memnode), uint64(2), uint64(2), new(sync.RWMutex)}
  ret.l.Lock()
  defer ret.l.Unlock()
  ret.nodes[uint64(1)] = &memnode { emptyDir(uint64(1)),new(sync.Mutex)}
  return ret
}

type wlwrapper struct {
  wlock *sync.Mutex
}

func (w wlwrapper) Unlock() error {
  return nil
}


//func NewMemNames() (names NameService) {
  //return *MemNames{make(map[string]PathEntry),make(map[uint64]Inode),uint64(0),sync.Mutex{}}
//}

func (n *MemNames) GetInode(nodeid uint64) (i *Inode, err error) {
  n.l.RLock()
  defer n.l.RUnlock()
  memnode := n.nodes[nodeid]
  if (memnode == nil) {
    return nil,errors.New("inode doesn't exist!")
  }
  memnode.mlock.Lock()
  defer memnode.mlock.Unlock()
  return memnode.node,nil
}

func (n *MemNames) AddInode(node Inode) (id uint64, err error) {
  n.l.Lock()
  defer n.l.Unlock()
  fmt.Printf("last id %d",n.inodeIdCounter)
  id = IncrementAndGet(&n.inodeIdCounter,uint64(1))
  fmt.Printf("new id %d" ,id)
  node.Inodeid = id
  n.nodes[id] = &memnode{ &node, new(sync.Mutex) }
  return id,nil
}

func (n *MemNames) StatFs() (stat syscall.Statfs_t, err error) {
  n.l.RLock()
  defer n.l.RUnlock()
  return syscall.Statfs_t{},nil
}


func (n *MemNames) Mutate(nodeid uint64, mutator func(inode *Inode) error) (newNode *Inode, err error) {
  n.l.RLock()
  defer n.l.RUnlock()
  mn := n.nodes[nodeid]
  mn.mlock.Lock()
  defer mn.mlock.Unlock()
  newNode = CopyInode(mn.node)
  fmt.Printf("Mutating node %+v\n",mn.node)
  err = mutator(newNode)
  fmt.Printf("AFTER MUTATE \n %+v \n",newNode)
  if (err != nil) { return nil,err }
  mn.node = newNode
  // TODO handle case where Nlinks becomes 0 -- or don't i guess, whatever, we'll just orphan them
  return mn.node,nil
}

func (n *MemNames) AddBlock(nodeid uint64, length uint32) (newBlock Block, err error) {
  newId := atomic.AddUint64(&n.blockIdCounter,uint64(1))
  node,err := n.Mutate(nodeid, func(inode *Inode) error {
    nextStartPos := uint64(0)
    nextEndPos := uint64(length)
    if (len(inode.Blocks) > 0) {
      lastBlock := inode.Blocks[len(inode.Blocks) - 1]
      // sanity check that last block is actually full
      if (lastBlock.EndPos - lastBlock.StartPos < BLOCKLENGTH) {
        return errors.New("Last block not quite full!")
      }
      nextStartPos = lastBlock.EndPos + uint64(1)
      nextEndPos = lastBlock.EndPos + uint64(length)
    }
    newBlock := Block {
      Id: newId,
      Mtime: uint64(time.Now().Unix()),
      Inodeid: inode.Inodeid,
      StartPos: nextStartPos,
      EndPos: nextEndPos, // size initially 0
      Container: uint32(0),
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

func (n *MemNames) ExtendBlock(nodeid uint64, blockId uint64, delta uint32) (newBlock Block, err error) {
  _,err = n.Mutate(nodeid, func(inode *Inode) error {
    var success bool
    for idx,blk := range inode.Blocks {
      if blk.Id == blockId {
        newBlock = blk
        newBlock.EndPos += uint64(delta)
        inode.Blocks[idx] = newBlock
        success = true
      }
    }
    if (! success) {
      return fmt.Errorf("No block with id %d attached to inode %d",blockId,nodeid)
    }
    return nil
  })
  // extend on data nodes
  err = n.dataNode.ExtendBlock(blockId,delta)
  if err != nil {
    return Block{},err
  }
  return newBlock,nil
}


