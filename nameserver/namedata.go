package nameserver

import (
  
  "github.com/jmhodges/levigo"
  "encoding/binary"
  "sync"
  "sync/atomic"
  
  "github.com/jbooth/maggiefs/maggiefs"
)

var (
  ReadOpts = levigo.NewReadOptions()
  WriteOpts = levigo.NewWriteOptions()
  OpenOpts = levigo.NewOptions()
)

// this struct is responsible for managing the on disk data underlying the inode system
// and the relationships between inodes and blocks
type NameData struct {
  
  maxInodeId uint64
  maxBlockId uint64
  maxDnId uint32
  inodeWriteLock map[uint64] *sync.Mutex
  blkWriteLock map[uint64] *sync.Mutex
  
  inodb *levigo.DB // inodeid -> inode
  allBlocks *levigo.DB // blockid -> block
  dnBlocks *levigo.DB // nodeIdBlockId (12 byte key) -> block, we can scan this to find all blocks belonging to a given datanode
  dnIds *levigo.DB // maps dn uint32 id -> hostname
}

// formats a new filesystem in the given data dir
func Format(dataDir string) error {
  return nil
}

// initializes a namedata
func NewNameData(dataDir string) (*NameData, error) {
  return nil,nil
}

// gets an inode in binary representation (call ToInode to turn into object)
func (nd *NameData) GetInode(inodeid uint64) ([]byte,error) {
  key := make([]byte,8)
  binary.LittleEndian.PutUint64(key,inodeid)
  ret,err := nd.inodb.Get(ReadOpts,key)
  return ret,err
}

// attempts to set an inode with the given inodeid, generationid to the value encoded in body -- returns true on success, false if generationid is too old
// this function is also used to add a new inode, just add it with generationid > 0
func (nd *NameData) SetInode(inodeid uint64, generationid uint64, body []byte) (bool,error) {
  key := make([]byte,8)
  binary.LittleEndian.PutUint64(key,inodeid)
  // lock for duration of our read/write
  nd.inodeWriteLock[inodeid % uint64(len(nd.inodeWriteLock))].Lock()
  defer nd.inodeWriteLock[inodeid % uint64(len(nd.inodeWriteLock))].Unlock()
  
  // now read to confirm correct generation id
  inodeBytes,err := nd.inodb.Get(ReadOpts,key)
  if err != nil { return false,err }
  var inode *maggiefs.Inode
  if len(inodeBytes) > 0 {
    inode = toInode(inodeBytes)
  } else {
    inode = &maggiefs.Inode{}
  }
  // if generation id is too old, send retry
  if inode.Generation > generationid { return false,nil }
  // else do the write and send OK
  err = nd.inodb.Put(WriteOpts,key,body)
  if err != nil { return false,err }
  return true,nil
}

// atomically adds incr to val, returns new val
func incrementAndGet(val *uint64, incr uint64) uint64 {
  currVal := atomic.LoadUint64(val)
  for ; !atomic.CompareAndSwapUint64(val,currVal,currVal+incr) ; {
    currVal = atomic.LoadUint64(val)
  }
  return currVal + incr
}
