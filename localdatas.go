package maggiefs

import (
  "os"
)

type LocalBlockReader struct {
  file *os.File
  currPageNum int
}

func (f LocalBlockReader) ReadPage(p []byte) (err error) {
  _,err = f.file.Read(p[0:int(PAGESIZE)])
  return err
}

func (f LocalBlockReader) SeekPage(pageNum int) error {
  _,err := f.file.Seek(int64(pageNum * int(PAGESIZE)), 0)
  return err
}

func (f LocalBlockReader) CurrPageNum() int {
  return f.currPageNum
}

func (f LocalBlockReader) Close() error {
  return f.file.Close()
}

type LocalBlockWriter struct {
  file *os.File
}

func (f LocalBlockWriter) WritePage(p []byte, pageNum int) error {
  _,err := f.file.WriteAt(p[0:4096], int64(pageNum * int(PAGESIZE)))
  return err
}

func (f LocalBlockWriter) Write(p []byte, pageNum int, off int, length int) error {
  fileOff := int64((pageNum * int(PAGESIZE)) + off)
  _,err := f.file.WriteAt(p[0:length], fileOff)
  return err
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
  file,err := os.Open(d.pathFor(blk.Id))
  return LocalBlockReader{file,0},err
}

func (d LocalDatas) Write(blk Block) (conn BlockWriter, err error) {
  file,err := os.Open(d.pathFor(blk.Id))
  return LocalBlockWriter{file},err
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

func (d LocalDatas) pathFor(blkId uint64) string {
  return d.baseDir + "/" + string(blkId)
}

func NewLocalDatas(baseDir string) *LocalDatas {
  return &LocalDatas{baseDir}

}
