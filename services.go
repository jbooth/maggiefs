package maggiefs

import (
  "syscall"
)
type NameService interface {
  
  GetPathInode(path string) (p PathEntry, i Inode, err error)
  GetInode(nodeid uint64) (i Inode, err error)
  StatFs() (statfs syscall.Statfs_t, err error)

    
}

type DataService interface {
}

// represents a session of interacting with a region
type BlockSession interface {

  NumBlocks() uint32
  // reads 1 page, will throw error of p is < 4096 bytes
  Read(p []byte) (n int, err error)

  // writes 1 page, will throw error if p is not exactly 4096 bytes
  Write(p []byte) (n int, err error)




}
