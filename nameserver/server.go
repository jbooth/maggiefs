package nameserver

import (
  "github.com/jmhodges/levigo"
  "net"
)

type NameServer struct {
  db levigo.DB
  l *net.TCPListener
  
}

type conn struct {
  c *net.TCPConn
}

func (c *conn) serve() {

}


func (ns *NameServer) accept() {


}


