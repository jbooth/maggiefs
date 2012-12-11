package nameserver

import (

  "github.com/jbooth/maggiefs/maggiefs"
)

type rawclient struct {
}

func (c *rawclient) getInode(nodeid uint64) (i *maggiefs.Inode, version uint64, err error) {
  return nil,0,nil
}

func (c *rawclient) setInode(i *maggiefs.Inode, lastVersion uint64) error {
  return nil
}