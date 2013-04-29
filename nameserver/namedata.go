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
  COUNTER_INODE = "INODES"
  COUNTER_BLOCK = "BLOCKS"
  COUNTER_VOLID = "VOLIDS"
  COUNTER_DNID = "DNIDS"
)

// this struct is responsible for managing the on disk data underlying the inode system
// and the relationships between inodes and blocks
type NameData struct {
  inodeStripeLock map[uint64] *sync.Mutex
  inodb *levigo.DB // inodeid -> inode
  counterLock *sync.Mutex
  counterdb *levigo.DB // counterName -> uint64
}
const STRIPE_SIZE = 1024 // must be power of 2

const dir_inodb = "inodes"
const dir_counters = "counters"

// formats a new filesystem in the given data dir
func Format(dataDir string) error {
  // wipe out previous
  err := os.RemoveAll(dataDir + "/" + dir_inodb)
  if err != nil { return err }
  err = os.RemoveAll(dataDir + "/" + dir_counters)
  if err != nil { return err }
  // create
  opts := levigo.NewOptions()
  defer opts.Close()
  opts.SetCreateIfMissing(true)
  
  db,err := levigo.Open(dataDir + "/" + dir_inodb, opts)
  if err != nil { return err }
  db.Close()
  db,err = levigo.Open(dataDir + "/" + dir_counters, opts)
  if err != nil { return err }
  db.Close()
  
  return nil
}

// initializes a namedata
func NewNameData(dataDir string) (*NameData, error) {
  opts := OpenOpts
  // todo configure caching
  // todo investigate turning off compression
  inodb, err := levigo.Open(dataDir + "/" + dir_inodb,opts)
  if err != nil { return nil,err }
  
  ret := &NameData{}
  ret.inodb = inodb
  ret.inodeStripeLock = make(map[uint64] *sync.Mutex)
  for i := uint64(0) ; i < STRIPE_SIZE ; i++ {
    ret.inodeStripeLock[i] = &sync.Mutex{}
  }
  // gotta set up blockid counter
  //ret.blockIdCounter = highestKey(ret.allBlocks)
  return ret,nil
}

func (nd *NameData) Close() {
  nd.inodb.Close()
}

func (nd *NameData) GetInode(inodeid uint64) (*maggiefs.Inode,error) {
  key := make([]byte,8)
  binary.LittleEndian.PutUint64(key,inodeid)
  bytes,err := nd.inodb.Get(ReadOpts,key)
  if len(bytes) == 0 { return nil,fmt.Errorf("No inode for id %d",inodeid) }
  ret := &maggiefs.Inode{}
  ret.FromBytes(bytes)
  return ret,err
}

// seta an inode
func (nd *NameData) SetInode(i *maggiefs.Inode) (err error) {
  nd.inodeStripeLock[i.Inodeid & STRIPE_SIZE].Lock()
  defer nd.inodeStripeLock[i.Inodeid & STRIPE_SIZE].Unlock()
  key := make([]byte,8)
  binary.LittleEndian.PutUint64(key,i.Inodeid)
  // do the write and send OK
  b,err := i.GobEncode()
  err = nd.inodb.Put(WriteOpts,key,b)
  return err
}

// adds an inode persistent store, setting its inode ID to the generated ID, and returning 
// the generated id and error
func (nd *NameData) AddInode(i *maggiefs.Inode) (uint64,error) {
  newNodeId,err := nd.GetIncrCounter(COUNTER_INODE,1)
  if err != nil {
    return 0,err
  }
  i.Inodeid = newNodeId
  err = nd.SetInode(i)
  return i.Inodeid,err
}

func (nd *NameData) DelInode(nodeid uint64) error {
	key := make([]byte,8)
  binary.LittleEndian.PutUint64(key,nodeid)
  return nd.inodb.Delete(WriteOpts,key)
}

func (nd *NameData) Mutate(inodeid uint64, f func(i *maggiefs.Inode) (error)) (*maggiefs.Inode,error) {
  nd.inodeStripeLock[inodeid & STRIPE_SIZE].Lock()
  defer nd.inodeStripeLock[inodeid & STRIPE_SIZE].Unlock()
  i,err := nd.GetInode(inodeid)
  if err != nil { return nil,err }
  err = f(i)
  if err != nil { return nil,err }
  err = nd.SetInode(i)
  return i,err
}

func (nd *NameData) GetBlock(inodeid uint64, blockid uint64) (*maggiefs.Block, error) {
  
  inode,err := nd.GetInode(inodeid)
  if err != nil { return nil,err }
  for _,b := range inode.Blocks {
    if b.Id == blockid {
      return &b,nil
    }
  }
  return nil,fmt.Errorf("No block id %d attached to inode %d",blockid,inodeid)
}

// adds a block to persistent store as the last block of inodeid, 
// setting its blockid to the generated ID, and returning the generated ID and error
func (nd *NameData) AddBlock(b maggiefs.Block, inodeid uint64) (uint64,error) {
    newBlockId,err := nd.GetIncrCounter(COUNTER_BLOCK,1)
    b.Id = newBlockId
    _,err = nd.Mutate(inodeid, func(i *maggiefs.Inode) (error) {
      i.Blocks = append(i.Blocks, b)
      return nil
    })
    if err != nil { return 0,err }
    return b.Id,nil
}

// gets and increments the counter, creating if necessary.  returns new val
func (nd *NameData) GetIncrCounter(counterName string, incr uint64) (uint64,error) {
  nd.counterLock.Lock()
  defer nd.counterLock.Unlock()
  key := []byte(counterName)
  valBytes,err := nd.counterdb.Get(ReadOpts,key)
  if err != nil {
    return 0,err
  }
  val := uint64(0)
  if (valBytes != nil && len(valBytes) == 8) {
    val = binary.LittleEndian.Uint64(valBytes)
  } else {
    valBytes = make([]byte,8)
  }
  val += incr
  binary.LittleEndian.PutUint64(valBytes,val)
  err = nd.counterdb.Put(WriteOpts,key,valBytes)
  return val,err
} 