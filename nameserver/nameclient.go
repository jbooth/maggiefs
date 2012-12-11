package nameserver

import (
	"github.com/jbooth/maggiefs/maggiefs"
	"syscall"
)

type NameClient struct {
	c rawclient
}

func (ns *NameClient) GetInode(nodeid uint64) (i *maggiefs.Inode, err error) {
	return nil, nil
}

func (ns *NameClient) StatFs() (statfs syscall.Statfs_t, err error) {
	return syscall.Statfs_t{}, nil
}

func (ns *NameClient) AddInode(node maggiefs.Inode) (id uint64, err error) {
	return 0, nil
}

func (ns *NameClient) Mutate(nodeid uint64, mutator func(inode *maggiefs.Inode) error) (newNode *maggiefs.Inode, err error) {
	return nil, nil
}

func (ns *NameClient) AddBlock(nodeid uint64, length uint32) (newBlock maggiefs.Block, err error) {
	return maggiefs.Block{}, nil
}

func (ns *NameClient) ExtendBlock(nodeid uint64, blockId uint64, delta uint32) (newBlock maggiefs.Block, err error) {
	return maggiefs.Block{}, nil
}
