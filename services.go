package maggiefs

import (
  "syscall"
)
type NameService interface {
  
  Format() (err error)
  GetChild(parentid uint64, name string) (i Inode, err error)
  GetInode(nodeid uint64) (i Inode, err error)
  StatFs() (statfs syscall.Statfs_t, err error)
  // persists a new inode to backing store
  AddInode(node Inode) (id uint64, err error)
  // updates existing inode
  SaveInode(node Inode) (err error)
  // links an entry
  Link(parent uint64, child uint64, name string)
  // unlinks an entry
  Unlink(parent uint64, name string)

  // persists a PathEntry
  AddPathEntry(pe PathEntry) (err error)
  RenamePathEntry(oldPath string, newPath string) (err error)
  LinkPathEntry(path string, nodeid uint64) (err error)
  // todo attrs
}

type DataService interface {
  GetConn(datanode string) (conn DNConn, err error)
}

// represents a session of interacting with a block of a file
// each 16MB block consists of 4096 pages of 4096 bytes apiece
// sessions are navigated by seeking to a page number and then 
// reading or writing full pages of 4096 bytes
type DNConn interface {

  // reads 1 page, will throw error of p is < 4096 bytes
  Read(blockid uint64, page uint64, p []byte) (err error)

  // writes 1 page, will throw error if p is not exactly 4096 bytes
  // if file is not yet this long, we will throw error
  Write(blockid uint64, page uint64, p []byte) (err error)

  // closes or returns to pool
  Close() (err error)
}
