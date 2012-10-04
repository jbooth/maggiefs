package maggiefs

import (
  "fmt"
  "errors"
)

func NewReader(inodeid uint64, names NameService, datas DataService) (r *Reader, err error) {
  return nil,nil
}

func NewWriter(inodeid uint64, names NameService, datas DataService) (w *Writer, err error) {
  return nil,nil
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
}

// reads UP TO length bytes from this inode
// may return early without error, so ensure you loop and call again
func (r *Reader) ReadAt(p []byte, offset uint64, length uint32) (n uint32, err error) {
  // have to re-get inode every time because it might have changed
  inode,err := r.names.GetInode(r.inodeid)
  if (err != nil) { return 0,err }
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
  if (offset >= inode.Length) { 
    panic(fmt.Sprintf("offset %d greater than block length %d", offset, inode.Length))
  }
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
  inode Inode
  globalPos uint64 // pos in file
  blockPos int // pos within block
  bufferPos int // pos within buffer
  currBlock Block
  currWriter BlockWriter
  names NameService
  datas DataService
}

//io.Writer
func (f *Writer) WriteAt(p []byte, off uint64, length uint64) (n int, err error) {
  return int(0),nil
}

func (f *Writer) Close() (err error) {
  return nil
}

