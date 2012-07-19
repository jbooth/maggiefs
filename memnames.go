package maggiefs

import (
  "syscall"
  "sync"
)

// implements nameservice
type MemNames struct {
  paths map[string]PathEntry
  nodes map[uint64]Inode
  idCounter uint64
  s sync.Mutex
}

func (m MemNames) Format() (err error) {
  return nil
}

func NewMemNames() (names NameService) {
  return MemNames{make(map[string]PathEntry),make(map[uint64]Inode),uint64(0),sync.Mutex{}}
}

func (n MemNames) GetPathInode(path string) (p PathEntry, i Inode, err error) {
  n.s.Lock()
  defer n.s.Unlock()
  return PathEntry{},Inode{},nil

}

func (n MemNames) GetPathEntry(path string) (p PathEntry, err error) {
  n.s.Lock()
  defer n.s.Unlock()
  return n.paths[path],nil
}

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

func (n MemNames) AddPathEntry(pe PathEntry) (err error) {
  n.s.Lock()
  defer n.s.Unlock()
  n.paths[pe.Path] = pe
  return nil
}

func (n MemNames) RenamePathEntry(oldPath string, newPath string) (err error) {
  n.s.Lock()
  defer n.s.Unlock()
  pe := n.paths[oldPath]
  delete(n.paths,oldPath)
  pe.Path = newPath
  n.paths[newPath] = pe
  return nil

}

func (n MemNames) LinkPathEntry(path string, nodeid uint64) (err error) {
  n.s.Lock()
  defer n.s.Unlock()
  //node Inode := 
  return nil
}

func (n MemNames) StatFs() (stat syscall.Statfs_t, err error) {
  n.s.Lock()
  defer n.s.Unlock()
  return syscall.Statfs_t{},nil
}
