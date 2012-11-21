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

// workflow

// main Serve() method accepts connections and launches their respective goroutines
// mux() pulls requests from conns and feeds them responses

type clientConn struct {
  c net.TCPConn
  d *gob.Decoder
  e *gob.Encoder
  req chan request
  resp chan response
  closed chan bool
}

func (c clientConn) readRequests() {
  for {
    req := request{}
    err := d.Decode(&req)
    if err != nil {
      // we should probably close here
      fmt.Printf("error reading from conn %s\n",err.String())
    }
    c.req <- req
  }

}

func (c clientConn) sendResponses() {
  for {
    resp := <- c.resp
    err := c.e.Encode(resp)
    if err != nil {
      // should prob signal close here
      fmt.Printf("error writing to conn %s\n",err.String())
    }
  }
}

func newClientConn(raw net.TCPConn) (clientConn, error) {
  err := raw.SetKeepAlive(true)
  if err != nil { return clientConn{},err }
  err = raw.SetNoDelay(true)
  if err != nil { return clientConn{},err }
  return clientConn{
    c:raw,
    d:gob.NewDecoder(raw),
    e:gob.NewEncoder(raw),
    req:make(chan request),
    resp:make(chan response)},nil
}

type LeaseServer struct {
  sock net.Listener
  newConns chan clientConn
  responses chan response
}

func (ls LeaseServer) Serve() {
  // start processing()
  go ls.process()
  // for new connection
  //    launch its goroutines
  for {
    tcpConn,err := ls.sock.AcceptTCP()
    if err != nil {
      fmt.Printf("error accepting connection: %s\n",err.String())
      return
    }
    client := newClientConn(tcpConn)
    go client.readRequests()
    go client.sendResponses()

  }
}

func (ls LeaseServer) mux() {
  
}

func (ls LeaseServer) Close() {
}
