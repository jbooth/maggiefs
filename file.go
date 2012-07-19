package maggiefs

import (
  "io"
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
  path PathEntry
  inode Inode
  buffer []byte 
  globalPos uint64 // pos in file
  blockPos uint32 // pos within block
  bufferPos uint32 // pos within buffer
  pageNum uint64  // current page we're reading
  datas DataService
  currBlock Block
  currConn DNConn
}

func (f *Reader) ReadAt(p []byte, offset uint64, size uint32) (n int, err error) {
  nRead := uint32(0)
  for ; nRead < off ; {
    // make sure we're at right block

    // figure out page to read and read it

    // copy to dest and update

  }
}
// io.Reader
func (f *Reader) Read(p []byte) (n int, err error) {
  nRead := uint32(0)
  numToRead := uint32(len(p))
  numLeftInFile := f.inode.Length - f.globalPos
  if (numLeftInFile == 0) { return 0,io.EOF }
  // shorten read to "rest of file" if necessary
  if (numLeftInFile < uint64(numToRead)) {
    numToRead = uint32(numLeftInFile)
  }
  var innerN uint32

  for ; (nRead < numToRead) ;{
    // make sure buffer has bytes, read more if we need to
    if (f.bufferPos == uint32(len(f.buffer))) {
      err = f.refillReadBuffer()
      if (err != nil) { return int(nRead),err }
    }

    // copy from buffer to dest, up to PAGESIZE bytes
    innerN = uint32(numToRead - nRead)
    if (innerN > PAGESIZE) { innerN = PAGESIZE }
    for i := uint32(0) ; i < innerN ; i++ {
      p[i] = f.buffer[i]
    }
    // end loop and repeat until done
    nRead += innerN
    f.globalPos += uint64(innerN)
    f.blockPos += uint32(innerN)
    f.bufferPos += innerN
  }
  return int(nRead),nil
}

func (f *Reader) refillReadBuffer() (err error) {
  // if we're at the end of the file, error
  if (f.globalPos == f.inode.Length) { return io.ErrUnexpectedEOF }
  // check if we need to move to the next block
  if (f.blockPos == BLOCKSIZE) {
   err := f.switchBlock(f.currBlock.NumInFile + 1)
   if (err != nil) { return err }
  }

  // read the buffer
  err = currConn.Read(currBlock.Inodeid, 
  if (err != nil) { return err }
  return f.currSession.Read(f.buffer)
}

func (f *Reader) switchBlock(blockNum uint64) error {
  err := f.currSession.Close()
  if (err != nil) { return err }
  f.currSession,err = f.datas.OpenBlock(f.inode.Blocks[blockNum])
  return err // hopefully nil
}


//io.Closer
func (f *Reader) Close() error {
  return nil
}

type Writer struct {
  path PathEntry
  inode Inode
  buffer []byte 
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

