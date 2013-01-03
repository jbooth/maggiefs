package nameserver

import (
	"github.com/jbooth/maggiefs/maggiefs"
	"syscall"
	"encoding/binary"
	"errors"
)

type NameClient struct {
	c *rawclient
}

func (ns *NameClient) GetInode(nodeid uint64) (i *maggiefs.Inode, err error) {
  // req body is empty, resp body is inode
  req := request{
    Op: OP_GETINODE,
    Inodeid: nodeid,
    Generation: 0,
    Body: []byte{},
  }
  resp,err := ns.c.doReq(req)
	return maggiefs.ToInode(resp.Body), err
}


func (ns *NameClient) AddInode(node *maggiefs.Inode) (id uint64, err error) {
	// req body is inode, resp body is little endian uint64
	req := request {
	 Op: OP_ADDINODE,
	 Inodeid: 0,
	 Generation: 0,
	 Body: maggiefs.FromInode(node),
	}
	resp,err := ns.c.doReq(req)
	if err != nil { return 0,err }
	// new id in resp body
	return binary.LittleEndian.Uint64(resp.Body),nil
}

func (ns *NameClient) Mutate(nodeid uint64, mutator func(inode *maggiefs.Inode) error) (newNode *maggiefs.Inode, err error) {
	// req body is inode, resp body is empty with success
	// we switch on resp.Stat and either retry or succeed
	success := false
	var inode *maggiefs.Inode
	for !success {
	  
	  
	  getReq := request {
	   Op: OP_GETINODE,
	   Inodeid: nodeid,
	   Generation: 0,
	   Body: []byte{},
	  }
	  getResp,err := ns.c.doReq(getReq)
	  if err != nil { return nil,err }
	  inode = maggiefs.ToInode(getResp.Body)
	  err = mutator(inode)
	  if err != nil { return nil,err }
	  setReq := request {
	   Op: OP_SETINODE,
	   Inodeid: inode.Inodeid,
	   Generation: inode.Generation,
	   Body: maggiefs.FromInode(inode),
	  }
	  setResp,err := ns.c.doReq(setReq)
	  if setResp.Status == STAT_OK { 
	    success = true 
	  }  else if setResp.Status == STAT_RETRY { 
	    continue 
	  } else if setResp.Status == STAT_ERR { 
	    return nil,errors.New(string(setResp.Body))
	  }
	}
	return inode, nil
}

  // Links the given child to the given parent, with the given name
func (ns *NameClient) Link(parent uint64, child uint64, name string) (newNode *maggiefs.Inode, err error) {
  return nil,nil
}
  // Unlinks the child with the given name
func (ns *NameClient) Unlink(parent uint64, name string) (newNode *maggiefs.Inode, err error) {
  return nil,nil
}

func (ns *NameClient) AddBlock(nodeid uint64, length uint32) (newBlock maggiefs.Block, err error) {
	// req body is empty, resp body is new block
	return maggiefs.Block{}, nil
}

func (ns *NameClient) ExtendBlock(nodeid uint64, blockId uint64, delta uint32) (newBlock maggiefs.Block, err error) {
	// req body is blockid,delta, resp body is new block
	return maggiefs.Block{}, nil
}


func (ns *NameClient) StatFs() (statfs syscall.Statfs_t, err error) {
  // req body is empty, resp body is statfs
  return syscall.Statfs_t{}, nil
}
