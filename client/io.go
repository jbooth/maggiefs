package client

import (
  "time"
  "io"
  "fmt"
  "errors"
  "sync"
  "github.com/jbooth/maggiefs/maggiefs"
)

const (
  PAGESIZE = maggiefs.PAGESIZE
  BLOCKLENGTH = maggiefs.BLOCKLENGTH
)

func NewReader(inodeid uint64, names maggiefs.NameService, datas maggiefs.DataService) (r *Reader, err error) {
  return &Reader { inodeid,names,datas,nonBlock(),nil,make([]byte,maggiefs.PAGESIZE,maggiefs.PAGESIZE),new(sync.Mutex)},nil
}

func NewWriter(inodeid uint64, names maggiefs.NameService, datas maggiefs.DataService) (w *Writer, err error) {
  inode,err := names.GetInode(inodeid)
  if (err != nil) { return nil,err }

  return &Writer{inode,nonBlock(),nil,names,datas,new(sync.Mutex)},nil
}

func nonBlock() maggiefs.Block {
  return maggiefs.Block{uint64(0),uint64(0),uint64(0),uint64(0),uint64(0),uint32(0)}
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

//io.Writer
func (w *Writer) WriteAt(p []byte, off uint64, length uint32) (written uint32, err error) {
  w.l.Lock()
  defer w.l.Unlock()
  // first figure which block we're supposed to be at
  // if offset is greater than length, we can't write (must append from eof)
  if (off > w.inode.Length) { return 0,errors.New("offset > length of file") }
  if (off == w.inode.Length) {
    currLen := w.currBlock.EndPos - w.currBlock.StartPos
    // we need to either extend current block or add a new one if we're just starting a file or at the end of current block
    if (off == 0 || currLen == BLOCKLENGTH) {
      fmt.Printf("adding block\n")
      // add a new block
      blockLength := length
      if (blockLength > uint32(BLOCKLENGTH) ) { blockLength = uint32(BLOCKLENGTH) }
      w.currBlock,err = w.names.AddBlock(w.inode.Inodeid,blockLength)
      if (err != nil) { return 0, err }
      w.inode.Blocks = append(w.inode.Blocks,w.currBlock)
      w.currWriter,err = w.datas.Write(w.currBlock)
      if (err != nil) { return 0,err }
      currLen = 0
    } else if (currLen == w.inode.Length) {
      fmt.Printf("adding to end of current block\n")
    }
    // extend currBlock to min(currLen + len, BLOCKLENGTH)
    maxAddedByte := BLOCKLENGTH - (currLen)
    if (uint64(length) > maxAddedByte) { length = uint32(maxAddedByte) }
    w.currBlock,err = w.names.ExtendBlock(w.inode.Inodeid,w.currBlock.Id,length)
    if (err != nil) { return 0,err }
    // patch up local model
    w.inode.Length += uint64(length)
  } else {
    // find existing block we are overwriting
    newBlk,err := blockForPos(off, w.inode)
    if err != nil { return 0,err }
    fmt.Printf("block for pos %+v\n",w.currBlock)
    if (newBlk.Id != w.currBlock.Id) {
      // switch blocks
      w.currBlock = newBlk
    }
  }
  // if writer isn't configured to this block, switch it
  if w.currWriter == nil || w.currWriter.BlockId() != w.currBlock.Id {
    // force any pending changes
    if w.currWriter != nil { w.currWriter.Close() }
    fmt.Printf("allocating writer for block %d inode %d\n",w.currBlock.Id,w.inode.Inodeid)
    w.currWriter,err = w.datas.Write(w.currBlock)
    if (err != nil) { return 0,err }
  }
  // now write pages
  written = uint32(0)
  // write first page if fragment
  pageNum := int(off / uint64(4096))
  firstPageOff := off & (uint64(4095)) // fast modulo 4096
  if (firstPageOff != 0) {
    firstPageLength := int(4096) - int(firstPageOff)
    err = w.currWriter.Write(p, pageNum, 0, firstPageLength)
    if (err != nil) { return 0,err }
    written += uint32(firstPageLength)
    pageNum++
  }

  // write whole pages into output buffer
  numWholePagesToWrite := (length - written) / PAGESIZE
  for ; numWholePagesToWrite > 0 ; numWholePagesToWrite-- {
    fmt.Printf("writing page to block %+v\n",w.currBlock)
    err = w.currWriter.WritePage(p[written:written+PAGESIZE],pageNum)
    if (err != nil) { return written,err }
    pageNum++
    written += PAGESIZE
  }

  // write last page if fragment
  if (written < length) {
    lastPageLen := int(length - written)

    fmt.Println("writing last page len %d \n",lastPageLen)
    fmt.Println("writing from slice %v at offsets %d %d\n",p,written,written+uint32(lastPageLen)-1)
    err = w.currWriter.Write(p[written:],pageNum,int(written),int(lastPageLen))
    written += uint32(lastPageLen)
    if (err != nil) { return written,err }
  }
  // update block with modified time
  w.currBlock.Mtime = uint64(time.Now().Unix())
  return written,nil
}

func (w *Writer) Fsync() (err error) {
  // update namenode with our inode's status
  if (w.names == nil) { fmt.Println("w.names nil wtf man") }
  newNode,err := w.names.Mutate(w.inode.Inodeid, func(i *maggiefs.Inode) error {
    i.Length = w.inode.Length
    currTime := time.Now().Unix()
    i.Mtime = currTime
    i.Ctime = currTime
    for idx,b := range(w.inode.Blocks) {
      if (idx < len(i.Blocks)) {
        i.Blocks[idx] = b
      } else {
        i.Blocks = append(i.Blocks,b)
      }
    }
    return nil
  })
  if (err != nil) { return err}
  w.inode = newNode
  return nil
}

func (f *Writer) Close() (err error) {
  return nil
}

