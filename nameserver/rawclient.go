package nameserver

import (
  "net"
  "encoding/gob"
)

type rawclient struct {
  pool chan *rawconn
}

type rawconn struct {
  c *net.TCPConn
  e *gob.Encoder
  d *gob.Decoder
}

func newRawConn(c *net.TCPConn) *rawconn {
  c.SetKeepAlive(true)
  c.SetNoDelay(true)
  return &rawconn{
    c,
    gob.NewEncoder(c),
    gob.NewDecoder(c),
  }
}

func newRawClient(nnHost string, poolSize int) (*rawclient,error) {
  raddr,err := net.ResolveTCPAddr("tcp",nnHost)
  if err != nil { return nil,err }
  laddr,_ := net.ResolveTCPAddr("tcp","127.0.0.1")
  
  pool := make(chan *rawconn,poolSize + 1)
  for i := 0 ; i < poolSize ; i++ {
    conn,err := net.DialTCP("tcp",laddr,raddr)
    if err != nil { return nil,err }
    pool <- newRawConn(conn)
  }
  return &rawclient{pool},nil
}

func (c *rawclient) doReq(r request) (response,error) {
  conn := <- c.pool
  defer c.returnConn(conn) 
  err := conn.e.Encode(r)
  if err != nil { return response{},err }
  resp := response{}
  err = conn.d.Decode(&resp)
  if err != nil { return response{},err }
  return resp,nil
}

func (c *rawclient) returnConn(conn *rawconn) {
  c.pool <- conn
}