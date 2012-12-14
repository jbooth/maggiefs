package nameserver

import (
	"github.com/jbooth/maggiefs/maggiefs"
	"syscall"
)

type NameClient struct {
	c rawclient
}

func (ns *NameClient) GetInode(nodeid uint64) (i *maggiefs.Inode, err error) {
  // req body is empty, resp body is inode
	return nil, nil
}

func (ns *NameClient) StatFs() (statfs syscall.Statfs_t, err error) {
	// req body is empty, resp body is statfs
	return syscall.Statfs_t{}, nil
}

func (ns *NameClient) AddInode(node maggiefs.Inode) (id uint64, err error) {
	// req body is inode, resp body is empty
	return 0, nil
}

func (ns *NameClient) Mutate(nodeid uint64, mutator func(inode *maggiefs.Inode) error) (newNode *maggiefs.Inode, err error) {
	// req body is inode, resp body is empty with success/fail and retry
	return nil, nil
}

func (ns *NameClient) AddBlock(nodeid uint64, length uint32) (newBlock maggiefs.Block, err error) {
	// req body is empty, resp body is new block
	return maggiefs.Block{}, nil
}

func (ns *NameClient) ExtendBlock(nodeid uint64, blockId uint64, delta uint32) (newBlock maggiefs.Block, err error) {
	// req body is blockid,delta, resp body is new block
	return maggiefs.Block{}, nil
}
