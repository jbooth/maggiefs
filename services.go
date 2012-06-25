package maggiefs

import (
  "syscall"
)
type NameService interface {
  
  GetPathInode(path string) (p PathEntry, i Inode, err error)
  GetInode(nodeid uint64) (i Inode, err error)
  StatFs() (statfs syscall.Statfs_t, err error)
  // persists an inode to backing store
  AddInode(node Inode) (err error)
  // persists a PathEntry
  AddPathEntry(pe PathEntry) (err error)
  RenamePathEntry(oldPath string, newPath string)
  LinkPathEntry(path string, nodeid uint64)
  // todo attrs
}

type DataService interface {
  OpenBlock(block Block) (session BlockSession, err error)
}

// represents a session of interacting with a block of a file
// each 16MB block consists of 4096 pages of 4096 bytes apiece
// sessions are navigated by seeking to a page number and then 
// reading or writing full pages of 4096 bytes
type BlockSession interface {

  NumBlocks() uint32
  // reads 1 page, will throw error of p is < 4096 bytes
  Read(p []byte) (err error)

  // writes 1 page, will throw error if p is not exactly 4096 bytes
  Write(p []byte) (err error)

  // seeks to the given page number
  Seek(pagenum uint32) error

  Close() error

}
