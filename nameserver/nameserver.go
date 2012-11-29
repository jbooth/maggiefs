package nameserver

import (

  "github.com/jbooth/maggiefs/maggiefs"
)

type baseclient struct {
}

func (b *baseclient) getInode(nodeid uint64) (i *maggiefs.Inode, version uint64, err error) {
  return nil,0,nil
}

func (b *baseclient) setInode(i *maggiefs.Inode, lastVersion uint64) error {
  return nil
}