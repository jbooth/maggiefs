package maggiefs

import (
  "syscall"
  "os"
  "fmt"
)

type LocalBlockReader struct {
  file *os.File
  currPageNum int
}
// reads some bytes
func (f LocalBlockReader) Read(p []byte, pos uint64, length uint64) (err error) {
  _,err = f.file.ReadAt(p[0:int(length)],int64(pos))
  return err
}
 


func (f LocalBlockReader) Close() error {
  return f.file.Close()
}

type LocalBlockWriter struct {
  file *os.File
  blockId uint64
}

func (f LocalBlockWriter) Write(p []byte, pos uint64) error {
  _,err := f.file.WriteAt(p, int64(pos))
  return err
}

func (f LocalBlockWriter) BlockId() uint64 {
  return f.blockId
}

func (f LocalBlockWriter) Sync() error {
  return f.file.Sync()
}

func (f LocalBlockWriter) Close() error {
  return f.file.Close()
}

type LocalDatas struct {
  baseDir string
}

func (d LocalDatas) Read(blk Block) (conn BlockReader, err error) {
  file,err := os.OpenFile(d.pathFor(blk.Id),syscall.O_RDONLY,0777)
  return LocalBlockReader{file,0},err
}

func (d LocalDatas) Write(blk Block) (conn BlockWriter, err error) {
  if (blk.Id == 0) { 
    return nil,fmt.Errorf("Bad blk descriptor %d in block %+v\n",blk.Id,blk)
  }
  fmt.Printf("opening file %s for block %d",d.pathFor(blk.Id),blk.Id)
  file,err := os.OpenFile(d.pathFor(blk.Id),syscall.O_RDWR,0777)
  return LocalBlockWriter{file,blk.Id},err
}

func (d LocalDatas) AddBlock(id uint64) error {
  f,err := os.Create(d.pathFor(id))
  if (err != nil) { return err }
  err = f.Close()
  return err
}

func (d LocalDatas) RmBlock(id uint64) error {
  return os.Remove(d.pathFor(id))
}

// os extends blocks for us do we need this even?
func (d LocalDatas) ExtendBlock(id uint64, delta uint32) error {
  return nil 
}

func (d LocalDatas) pathFor(blkId uint64) string {
  return d.baseDir + "/" + fmt.Sprintf("%d",blkId)
}

func NewLocalDatas(baseDir string) *LocalDatas {
  return &LocalDatas{baseDir}

}
