package maggiefs

import (
  "syscall"
  "sync"
)

// implements nameservice
type MemNames struct {
  nodes map[uint64]Inode
  idCounter uint64
  s sync.Mutex
}

func (m MemNames) Format() (err error) {
  return nil
}

//func NewMemNames() (names NameService) {
  //return MemNames{make(map[string]PathEntry),make(map[uint64]Inode),uint64(0),sync.Mutex{}}
//}

func (n MemNames) GetInode(nodeid uint64) (i Inode, err error) {
  n.s.Lock()
  defer n.s.Unlock()
  return n.nodes[nodeid],nil
}

func (n MemNames) AddInode(node Inode) (id uint64, err error) {
  n.s.Lock()
  defer n.s.Unlock()
  id = n.idCounter + uint64(1)
  node.Inodeid = id
  n.nodes[id] = node
  return id,nil
}

func (n MemNames) StatFs() (stat syscall.Statfs_t, err error) {
  n.s.Lock()
  defer n.s.Unlock()
  return syscall.Statfs_t{},nil
}
