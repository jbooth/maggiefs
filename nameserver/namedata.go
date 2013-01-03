package nameserver

import (
  "os"
  "github.com/jmhodges/levigo"
  "encoding/binary"
  "sync"
  "fmt"
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
  inodeIdCounter uint64
  blockIdCounter uint64
  inodeStripeLock map[uint64] *sync.RWMutex
  blkStripeLock map[uint64] *sync.RWMutex
  
  inodb *levigo.DB // inodeid -> inode
  allBlocks *levigo.DB // blockid -> block
  dnBlocks *levigo.DB // nodeIdBlockId (12 byte key) -> block, we can scan this to find all blocks belonging to a given datanode
  dnIds *levigo.DB // maps dn uint32 id -> hostname
}
const STRIPE_SIZE = 1024 // must be power of 2

const dir_inodb = "inodb"
const dir_allBlocks = "allBlocks"
const dir_dnBlocks = "dnBlocks"
const dir_dnIds = "dnIds"

// formats a new filesystem in the given data dir
func Format(dataDir string) error {
  // wipe out previous
  err := os.RemoveAll(dataDir + "/" + dir_inodb)
  if err != nil { return err }
  err = os.RemoveAll(dataDir + "/" + dir_allBlocks)
  if err != nil { return err }
  err = os.RemoveAll(dataDir + "/" + dir_dnBlocks)
  if err != nil { return err }
  err = os.RemoveAll(dataDir + "/" + dir_dnIds)
  if err != nil { return err }
  // create
  opts := levigo.NewOptions()
  defer opts.Close()
  opts.SetCreateIfMissing(true)
  
  db,err := levigo.Open(dataDir + "/" + dir_inodb, opts)
  if err != nil { return err }
  db.Close()
  db,err = levigo.Open(dataDir + "/" + dir_allBlocks, opts)
  if err != nil { return err }
  db.Close()
  db,err = levigo.Open(dataDir + "/" + dir_dnBlocks,opts)
  if err != nil { return err }
  db.Close()
  db,err = levigo.Open(dataDir + "/" + dir_dnIds,opts)
  if err != nil { return err }
  db.Close()
  
  return nil
}

// initializes a namedata
func NewNameData(dataDir string) (*NameData, error) {
  opts := levigo.NewOptions()
  inodb, err := levigo.Open(dataDir + "/" + dir_inodb,opts)
  if err != nil { return nil,err }
  allBlocks, err := levigo.Open(dataDir + "/" + dir_allBlocks,opts)
  if err != nil { return nil,err }
  dnBlocks, err := levigo.Open(dataDir + "/" + dir_dnBlocks,opts)
  if err != nil { return nil,err }
  dnIds, err := levigo.Open(dataDir + "/" + dir_dnIds,opts)
  if err != nil { return nil,err }
  
  ret := &NameData{}
  ret.inodb = inodb
  ret.allBlocks = allBlocks
  ret.dnBlocks = dnBlocks
  ret.dnIds = dnIds
  
  return ret,nil
}

// scans the uint64 keys of the indicated db, returning the highest one by value
// scans all keys rather than using a comaprator
func highestKey(db *levigo.DB) (uint64,error) {
  opts := levigo.NewReadOptions()
  defer opts.Close()
  //iter := db.NewIterator()
  return 0,nil
}

// gets an inode in binary representation (call maggiefs.ToInode to turn into object)
func (nd *NameData) GetInode(inodeid uint64) ([]byte,error) {
  nd.inodeStripeLock[inodeid & STRIPE_SIZE].RLock()
  defer nd.inodeStripeLock[inodeid & STRIPE_SIZE].RUnlock()
  key := make([]byte,8)
  binary.LittleEndian.PutUint64(key,inodeid)
  ret,err := nd.inodb.Get(ReadOpts,key)
  if len(ret) == 0 { return nil,fmt.Errorf("No inode for id %d",inodeid) }
  return ret,err
}

// attempts to set an inode with the given inodeid, generationid to the value encoded in body -- returns true on success, false if generationid is too old
// this function is also used to add a new inode, just add it with generationid > 0
func (nd *NameData) SetInode(inodeid uint64, generationid uint64, body []byte) (bool,error) {
  key := make([]byte,8)
  binary.LittleEndian.PutUint64(key,inodeid)
  // lock for duration of our read/write
  nd.inodeStripeLock[inodeid & STRIPE_SIZE].Lock()
  defer nd.inodeStripeLock[inodeid & STRIPE_SIZE].Unlock()
  
  // now read to confirm correct generation id
  inodeBytes,err := nd.inodb.Get(ReadOpts,key)
  if err != nil { return false,err }
  if len(inodeBytes) > 0 {
    inode := maggiefs.ToInode(inodeBytes)
    // if generation id is too old, send retry
    if inode.Generation > generationid { return false,nil }
  } 
  // else do the write and send OK
  err = nd.inodb.Put(WriteOpts,key,body)
  if err != nil { return false,err }
  return true,nil
}

// adds a new inode with an unused inodeid and returns that inodeid
func (nd *NameData) AddInode(i *maggiefs.Inode) (uint64,error) {
  var success = false
  var err error
  var id uint64
  for !success {
    i.Inodeid = maggiefs.IncrementAndGet(&nd.inodeIdCounter,1)
    inoBytes := maggiefs.FromInode(i)
    success,err = nd.SetInode(i.Inodeid,1,inoBytes)
    if err != nil { return 0,err }
  }
  return id,nil
}

func (nd *NameData) AddBlock(b maggiefs.Block) {

}

