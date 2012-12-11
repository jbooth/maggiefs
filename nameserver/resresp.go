package nameserver

import (
  "github.com/jbooth/maggiefs/maggiefs"
)

const (
  OP_GETINODE int32 = iota
  OP_SETINODE int32 = iota
  
  STAT_OK byte = 0
  STAT_ERR byte = 1
  STAT_TOOOLD byte = 2
  
)

type request struct {
  op int32
  inodeid uint64
  generation uint64
  Inode *maggiefs.Inode 
}

type response struct {
  status int32
  Inode *maggiefs.Inode  
}
