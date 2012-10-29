package client

import (
  "io"
  "fmt"
  "errors"
  "sync"
  "github.com/jbooth/maggiefs/maggiefs"
)


func NewReader(inodeid uint64, names maggiefs.NameService, datas maggiefs.DataService) (r *Reader, err error) {
  return &Reader { inodeid,names,datas,nonBlock(),nil,make([]byte,maggiefs.PAGESIZE,maggiefs.PAGESIZE),new(sync.Mutex)},nil
}


type brentry struct {
  r maggiefs.BlockReader
  blockId uint64
  lastUsedTxn uint64
}
// used by reader to store recently used block readers
type brCache struct {
  lock sync.Mutex // maybe make this a spinlock later
  readers []brentry
  txnCount uint64
}

func (br brCache) get() {
}

func (br brCache) ret(brentry) {
}


// represents an open file
// maintains a one page buffer for reading
// for writable or RW files, see OpenWriteFile
type Reader struct {
  inodeid uint64
  names maggiefs.NameService
  datas maggiefs.DataService
  currBlock maggiefs.Block
  currReader maggiefs.BlockReader
  pageBuff []byte
  l *sync.Mutex
}

// reads UP TO length bytes from this inode
// may return early without error, so ensure you loop and call again
func (r *Reader) ReadAt(p []byte, offset uint64, length uint32) (n uint32, err error) {
  r.l.Lock()
  defer r.l.Unlock() // TODO get lockless on this 
  // have to re-get inode every time because it might have changed
  inode,err := r.names.GetInode(r.inodeid)
  if (err != nil) { return 0,err }
  if (offset == inode.Length) { return 0,io.EOF }
  if (offset > inode.Length) { return 0,errors.New("Read past end of file") }
  // confirm currBlock and currReader correct
  nRead := uint32(0)
  blockNeeded,err := blockForPos(offset + uint64(nRead), inode)
  if (err != nil) { return 0,err }
  if (r.currReader != nil && blockNeeded.Id == r.currBlock.Id && blockNeeded.Mtime == r.currBlock.Mtime) {
    // currBlock is good, stick with currReader
  } else {
    // close old one (presumably returns to pool)
    if (r.currReader != nil) {
      err = r.currReader.Close()
      if (err != nil) { return 0,err }
    }
    // initialize new blockReader
    r.currBlock = blockNeeded
    r.currReader,err = r.datas.Read(r.currBlock)
    if (err != nil) { return 0,err }
  }
  // if we're being asked to read past end of block, we just return early
  numBytesFromBlock := r.currBlock.EndPos - offset
  if (uint32(numBytesFromBlock) > length) { length = uint32(numBytesFromBlock) }

  // read first page if fragment
  firstPageOff := offset & (uint64(4095)) // fast modulo 4096
  if (firstPageOff != 0) {
    desiredPage := int(offset / uint64(4096))
    if (r.currReader.CurrPageNum() != desiredPage) {
      r.currReader.SeekPage(desiredPage)
    }
    err = r.currReader.ReadPage(r.pageBuff)
    if (err != nil) { return 0,err }
    firstPageLength := int(4096) - int(firstPageOff)
    copy(p[0:firstPageLength],r.pageBuff[firstPageOff:int(firstPageOff)+firstPageLength])
    nRead += uint32(firstPageLength)
  }

  // read whole pages into output buffer
  numWholePagesToRead := (length - nRead) / uint32(4096)
  for ; numWholePagesToRead > 0 ; numWholePagesToRead-- {
    err = r.currReader.ReadPage(p[nRead:nRead+maggiefs.PAGESIZE])
    if (err != nil) { return nRead,err }
    nRead += maggiefs.PAGESIZE
  }

  // read last page if fragment
  if (nRead < length) {
    r.currReader.ReadPage(r.pageBuff)
    copy(p[nRead:length],r.pageBuff[0:length-nRead])
  }
  return nRead,nil
}

func blockForPos(offset uint64, inode *maggiefs.Inode) (blk maggiefs.Block, err error) {
  for i := 0 ; i < len(inode.Blocks) ; i++ {
    blk := inode.Blocks[i]
    if (offset >= blk.StartPos && offset < blk.EndPos) {
      return blk,nil
    }
  }
  return maggiefs.Block{},errors.New(fmt.Sprintf("offset %d not found in any blocks for inode %d, bad file?", offset, inode.Inodeid))

}

//io.Closer
func (r *Reader) Close() error {
  if (r.currReader != nil) { r.currReader.Close() }
  return nil
}

type Writer struct {
  inode *maggiefs.Inode
  currBlock maggiefs.Block
  currWriter maggiefs.BlockWriter
  names maggiefs.NameService
  datas maggiefs.DataService
  l *sync.Mutex
}