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
