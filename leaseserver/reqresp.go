package leaseserver

import (

)

const(
  OP_READLEASE = iota
  OP_READLEASE_RELEASE = iota
  OP_WRITELEASE = iota
  OP_WRITELEASE_RELEASE = iota
  OP_WRITELEASE_COMMIT = iota
  OP_CHECKLEASES = iota

  STATUS_OK byte = 0
  STATUS_ERR byte = 1
  STATUS_WAIT byte = 2
  STATUS_NOTIFY byte = 3
)

type request struct {
  op byte
  leaseid uint64
  inodeid uint64
  reqno uint32 // sent back with response so we know which request it was
}

type response struct {
  reqno uint32 // reqno that was sent with the request, 0 if a notify 
  leaseid uint64
  inodeid uint64
  status byte // ok, err, or we're a notify
}




