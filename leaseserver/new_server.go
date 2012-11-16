package leaseserver

import (
  "sync"
  "net"
  "encoding/gob"
)

type checkedOutReadLease struct {
  l sync.Mutex
}

type leaseContainer struct {
}


// maps

// host -> list of pointers to inode/lease pairs

// inode -> list of readleases/host + writelease/host


// on actions

// readlease()  -- set it up, return unique id maybe?
// readlease.release() -- remove from this inode with this id
// writelease() -- set it up
// writelease.release() -- remove from this inode
// writelease.commit() -- notify all readleases

type clientConn struct {
  c net.TCPConn
  d *gob.Decoder
}

func newClientConn(raw net.TCPConn) (clientConn, error) {
  raw.SetKeepAlive(true)
  raw.SetNoDelay(true)
  return clientConn{raw,gob.NewDecoder(raw)},nil
}

type LeaseServer struct {
  sock net.Listener
}

func (ls LeaseServer) Serve() {
  // start muxer()
  go ls.mux()
  // for new connection
  //    add to mux
}

func (ls LeaseServer) mux() {
  
}

func (ls LeaseServer) Close() {
}