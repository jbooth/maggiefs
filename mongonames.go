package maggiefs

import (
  "syscall"
  "labix.org/v2/mgo"
//  "labix.org/v2/mgo/bson"
)

// implements nameservice
type MongoNames struct {
  mongo mgo.Session
  colPrefix string // collection prefix to identify collections belonging to this FS
}

func (m MongoNames) Format() (err error) {
  return nil

}
func NewMongoNames(connString string, colPrefix string) (names NameService, err error) {
  return MongoNames{},nil
}

func (n MongoNames) GetPathInode(path string) (p PathEntry, i Inode, err error) {
  return PathEntry{},Inode{},nil

}

func (n MongoNames) GetPathEntry(path string) (p PathEntry, err error) {
  return PathEntry{},nil
}

func (n MongoNames) GetInode(nodeid uint64) (i Inode, err error) {
  s := n.mongo.New()
  defer s.Close()

  return Inode{},nil

}

func (n MongoNames) AddInode(node Inode) (id uint64, err error) {
  return uint64(0),nil

}

func (n MongoNames) AddPathEntry(pe PathEntry) (err error) {
  return nil
}

func (n MongoNames) RenamePathEntry(oldPath string, newPath string) (err error) {
  return nil

}

func (n MongoNames) LinkPathEntry(path string, nodeid uint64) (err error) {
  return nil
}

func (n MongoNames) StatFs() (stat syscall.Statfs_t, err error) {
  return syscall.Statfs_t{},nil
}
