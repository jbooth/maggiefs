package leaseserver

import (

)

const(
  OP_READLEASE = iota
  OP_READLEASE_RELEASE = iota
  OP_WRITELEASE = iota
  OP_WRITELEASE_RELEASE = iota
  OP_WRITELEASE_COMMIT = iota
  
  STATUS_OK byte = 0
  STATUS_ERR byte = 1
  STATUS_NOTIFY byte = 2
)

type request struct {
  op byte
  arg uint64 // arg is either an inode id or a lease id
  reqno uint64 // sent back with response so we know which request it was
}

type response struct {
  reqno uint64 // 
  leaseid uint64 // if allocating new lease, here's your id, otherwise it's an inode notification
  status byte // ok, err, or we're a notify
}




