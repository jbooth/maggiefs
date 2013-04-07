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
  inodeStripeLock map[uint64] *sync.Mutex
  hintInodeGC chan uint64
  inodb *levigo.DB // inodeid -> inode
  volBlocks *levigo.DB // volIdBlockId (12 byte key) -> inodeid (8 byte value), we can scan this to find all blocks belonging to a given volume
  volumes *levigo.DB // maps dn uint32 volume id  -> volume
}
const STRIPE_SIZE = 1024 // must be power of 2

const dir_inodb = "inodes"
const dir_volBlocks = "volBlocks"
const dir_volumes = "volumes"

// formats a new filesystem in the given data dir
func Format(dataDir string) error {
  // wipe out previous
  err := os.RemoveAll(dataDir + "/" + dir_inodb)
  if err != nil { return err }
  err = os.RemoveAll(dataDir + "/" + dir_volBlocks)
  if err != nil { return err }
  err = os.RemoveAll(dataDir + "/" + dir_volumes)
  if err != nil { return err }
  // create
  opts := levigo.NewOptions()
  defer opts.Close()
  opts.SetCreateIfMissing(true)
  
  db,err := levigo.Open(dataDir + "/" + dir_inodb, opts)
  if err != nil { return err }
  db.Close()
  db.Close()
  db,err = levigo.Open(dataDir + "/" + dir_volBlocks,opts)
  if err != nil { return err }
  db.Close()
  db,err = levigo.Open(dataDir + "/" + dir_volumes,opts)
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
  volBlocks, err := levigo.Open(dataDir + "/" + dir_volBlocks,opts)
  if err != nil { return nil,err }
  volumes, err := levigo.Open(dataDir + "/" + dir_volumes,opts)
  if err != nil { return nil,err }
  
  ret := &NameData{}
  ret.inodb = inodb
  ret.volBlocks = volBlocks
  ret.volumes = volumes
  ret.inodeStripeLock = make(map[uint64] *sync.Mutex)
  for i := uint64(0) ; i < STRIPE_SIZE ; i++ {
    ret.inodeStripeLock[i] = &sync.Mutex{}
  }
  ret.inodeIdCounter = highestKey(ret.inodb)
  // gotta set up blockid counter
  //ret.blockIdCounter = highestKey(ret.allBlocks)
  return ret,nil
}

func (nd *NameData) Close() {
  nd.inodb.Close()
  nd.volBlocks.Close()
  nd.volumes.Close()
}

// scans the uint64 keys of the indicated db, returning the highest one by value
// scans all keys rather than using a comparator, this is slow, replace later
func highestKey(db *levigo.DB) uint64 {
  opts := levigo.NewReadOptions()
  defer opts.Close()
  opts.SetFillCache(false)
  iter := db.NewIterator(opts)
  highest := uint64(0)
  for iter.Valid() {
    key := binary.LittleEndian.Uint64(iter.Key())
    if key > highest { highest = key }
  }
  return highest
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
  err = nd.inodb.Put(WriteOpts,key,i.ToBytes())
  return err
}

// adds an inode persistent store, setting its inode ID to the generated ID, and returning 
// the generated id and error
func (nd *NameData) AddInode(i *maggiefs.Inode) (uint64,error) {
  i.Inodeid = maggiefs.IncrementAndGet(&nd.inodeIdCounter,1)
  err := nd.SetInode(i)
  return i.Inodeid,err
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
    b.Id = maggiefs.IncrementAndGet(&nd.blockIdCounter,1)
    _,err := nd.Mutate(inodeid, func(i *maggiefs.Inode) (error) {
      i.Blocks = append(i.Blocks, b)
      return nil
    })
    if err != nil { return 0,err }
    return b.Id,nil
}
