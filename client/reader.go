package client

import (
  "io"
  "fmt"
  "errors"
  "github.com/jbooth/maggiefs/maggiefs"
)


func NewReader(inodeid uint64, names maggiefs.NameService, datas maggiefs.DataService) (r *Reader, err error) {
  return &Reader { inodeid,names,datas}, nil
}

// represents an open file
// maintains a one page buffer for reading
// for writable or RW files, see OpenWriteFile
type Reader struct {
  inodeid uint64
  names maggiefs.NameService
  datas maggiefs.DataService
}

// reads UP TO length bytes from this inode, from position into the provided array at offset
// may return early without error, so ensure you loop and call again
func (r *Reader) ReadAt(p []byte, position uint64, offset uint32, length uint32) (n uint32, err error) {
  // have to re-get inode every time because it might have changed
  inode,err := r.names.GetInode(r.inodeid)
  if (err != nil) { return 0,err }
  if (position == inode.Length) { return 0,io.EOF }
  if (position > inode.Length) { return 0,errors.New("Read past end of file") }
  // confirm currBlock and currReader correct
  block,err := blockForPos(position, inode)
  if (err != nil) { return 0,err }
  // get our blockreader  
  blockReader,err := r.datas.Read(block)
  defer blockReader.Close()
  // read at most the bytes remaining in this block
  // if we're being asked to read past end of block, we just return early
  numBytesFromBlock := block.EndPos - position
  if (uint32(numBytesFromBlock) > length) { length = uint32(numBytesFromBlock) }
  // read bytes
  posInBlock := uint64(offset) - block.StartPos
  err = blockReader.Read(p[offset:],posInBlock,numBytesFromBlock)
  return uint32(numBytesFromBlock),err
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
  return nil
}
