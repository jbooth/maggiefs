package maggiefs

import (
  "fmt"
  "errors"
)


func NewReader(inode Inode, datas DataService) (r *Reader, err error) {
  return nil,nil
}

func NewWriter(inode Inode, datas DataService) (w *Writer, err error) {
  return nil,nil
}
// represents an open file
// maintains a one page buffer for reading
// for writable or RW files, see OpenWriteFile
type Reader struct {
  inode Inode
  datas DataService
}

func (f *Reader) ReadAt(p []byte, offset uint64, length uint32) (n uint32, err error) {
  nRead := uint32(0)
  // loop until done
  for ; nRead < length ; {
    // get conn to block
    block,err := blockForPos(offset + uint64(nRead), f.inode)

    if (err != nil) { 
      return nRead,err 
    }
    // read
    blockPos := offset - block.StartPos
    numToReadFromBlock := uint32(block.EndPos - blockPos)
    if (numToReadFromBlock > length) { numToReadFromBlock = length }
    conn,err := f.datas.Read(block)
    if (err != nil) { return nRead,err }
    defer conn.Close()
    err = conn.Read(uint32(blockPos),numToReadFromBlock,p)
    if (err != nil) { return nRead,err }
    nRead += numToReadFromBlock
  }
  return nRead,nil
}

func blockForPos(offset uint64, inode Inode) (blk Block, err error) {
  if (offset >= inode.Length) { 
    return Block{}, errors.New(fmt.Sprintf("offset %d greater than block length %d", offset, inode.Length))
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
func (f *Reader) Close() error {
  return nil
}

type Writer struct {
  inode Inode
  globalPos uint64 // pos in file
  blockPos int // pos within block
  bufferPos int // pos within buffer
  currBlock Block
  datas DataService
}

//io.Writer
func (f *Writer) Write(p []byte) (n int, err error) {
  return int(0),nil
}

func (f *Writer) Close() (err error) {
  return nil
}

