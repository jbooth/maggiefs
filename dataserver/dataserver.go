package dataserver

import (
  "net"
  "github.com/jmhodges/levigo"
)

type DataServer struct {
  blockData *levigo.DB
  acceptSock *net.TCPListener
  
}

func NewDataServer(config *DSConfig) {

}


