package maggiefs

import (
  "io"
  "fmt"
  "errors"
  "sync"
)

func NewReader(inodeid uint64, names NameService, datas DataService) (r *Reader, err error) {
  return nil,nil
}

func NewWriter(inodeid uint64, names NameService, datas DataService) (w *Writer, err error) {
  return nil,nil
}

type brentry struct {
  r BlockReader
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
  names NameService
  datas DataService
  currBlock Block
  currReader BlockReader
  pageBuff []byte
  l sync.Mutex
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
  if (blockNeeded.Id == r.currBlock.Id && blockNeeded.Mtime == r.currBlock.Mtime) {
    // currBlock is good, stick with currReader
  } else {
    // close old one (presumably returns to pool)
    err = r.currReader.Close()
    // initialize new blockReader
    if (err != nil) { return 0,err }
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
    err = r.currReader.ReadPage(p[nRead:nRead+PAGESIZE])
    if (err != nil) { return nRead,err }
    nRead += PAGESIZE
  }

  // read last page if fragment
  if (nRead < length) {
    r.currReader.ReadPage(r.pageBuff)
    copy(p[nRead:length],r.pageBuff[0:length-nRead])
  }
  return nRead,nil
}

func blockForPos(offset uint64, inode *Inode) (blk Block, err error) {
  for i := 0 ; i < len(inode.Blocks) ; i++ {
    blk := inode.Blocks[i]
    if (offset > blk.StartPos && offset < blk.EndPos) {
      return blk,nil
    }
  }
  return Block{},errors.New(fmt.Sprintf("offset %d not found in any blocks for inode %d, bad file?", offset, inode.Inodeid))

}

//io.Closer
func (r *Reader) Close() error {
  r.currReader.Close()
  return nil
}

type Writer struct {
  inode *Inode
  currBlock Block
  currWriter BlockWriter
  names NameService
  datas DataService
  l sync.Mutex
}

//io.Writer
func (w *Writer) WriteAt(p []byte, off uint64, length uint32) (written uint32, err error) {
  w.l.Lock()
  defer w.l.Unlock()
  // if offset is greater than length, we can't write (must append or whatever)
  if (off > w.inode.Length) { return 0,errors.New("offset > length of file") }
  if (off == w.inode.Length) {
    currLen := w.currBlock.EndPos - w.currBlock.StartPos
    // we need to either extend current block or add a new one
    if (currLen == BLOCKLENGTH) {
      // add a new block
      w.currBlock,err = w.names.AddBlock(w.inode.Inodeid)
      if (err != nil) { return 0, err }
      w.inode.Blocks = append(w.inode.Blocks,w.currBlock)
      w.currWriter,err = w.datas.Write(w.currBlock)
      if (err != nil) { return 0,err }
      currLen = 0
    } 
    // extend currBlock to min(currLen + len, BLOCKLENGTH)
    maxAddedByte := BLOCKLENGTH - (currLen)
    if (uint64(length) > maxAddedByte) { length = uint32(maxAddedByte) }
    w.currBlock.EndPos += uint64(length)
    w.inode.Length += uint64(length)
  } else {
    // find existing block we are overwriting
    newBlk,err := blockForPos(off, w.inode)
    if (newBlk.Id != w.currBlock.Id) {
      // switch blocks
      w.currBlock = newBlk
      w.currWriter,err = w.datas.Write(w.currBlock)
      if (err != nil) { return 0,err }
    }
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

  // read whole pages into output buffer
  numWholePagesToRead := (length - written) / PAGESIZE
  for ; numWholePagesToRead > 0 ; numWholePagesToRead-- {
    err = w.currWriter.WritePage(p[written:written+PAGESIZE],pageNum)
    if (err != nil) { return written,err }
    pageNum++
    written += PAGESIZE
  }

  // write last page if fragment
  if (written < length) {
    lastPageLen := int(length - written)
    err = w.currWriter.WritePage(p[written:written+uint32(lastPageLen)],pageNum)
    if (err != nil) { return written,err }
  }
  return written,nil
}

func (w *Writer) Fsync() (err error) {
  return nil
}

func (f *Writer) Close() (err error) {
  return nil
}

