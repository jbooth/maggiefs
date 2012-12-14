package nameserver

import (
  "fmt"
  "net"
  "encoding/gob"
  "sync"
  "github.com/jbooth/maggiefs/maggiefs"
)

// we need to keep
//  leveldb of inodes
//  leveldb of blocks
//  global system statsfs

// additionally
//   connections to data servers
//   poll them to update stats



type NameServer struct {
  replicationFactor int
  dataNodes map[uint32] datanodeStat
  nd *NameData
  l *net.TCPListener
}

type datanodeStat struct {
  conn maggiefs.NameDataIface
  stat maggiefs.DataNodeStat
  l *sync.Mutex
}

// returns a slice of hosts for a new block, should be ns.replicationFactor in length
func (ns *NameServer) HostsForNewBlock() []uint32 {
  // todo, combo of free space and round robin
  return []uint32{}
}



type conn struct {
  c *net.TCPConn
  d *gob.Decoder
  e *gob.Encoder
  ns *NameServer
}

func (c conn) serve() {
  req := request{}
  for {
    // read request
    c.d.Decode(&req)
    var err error = nil
    resp := response{STAT_OK,[]byte{}}
    // handle
    
    switch (req.Op) {
      
      case OP_GETINODE:
        resp.Body,err = c.ns.nd.GetInode(req.Inodeid)
      case OP_SETINODE:
        var success bool
        success,err = c.ns.nd.SetInode(req.Inodeid,req.Generation,req.Body)
        if !success { resp.Status = STAT_RETRY }
        
      // todo:
      // link
      // unlink
      // addblock
      // extendblock
    }
    
    // send response
    if err != nil { 
      resp.Status = STAT_ERR
      fmt.Printf("error handling req %+v : %s",req,err)
    } 
    err = c.e.Encode(resp)
    if err != nil {
      fmt.Printf("Error sending response %+v : %s",resp,err)
    }
  }
}

func (ns *NameServer) accept() {
  for {
    c,err := ns.l.AcceptTCP()
    if err != nil {
      fmt.Printf("Error accepting!! %s\nExiting.\n",err)
      panic(err)
    }
    conn := conn {
      c,
      gob.NewDecoder(c),
      gob.NewEncoder(c),
      ns,
    }
    go conn.serve()
  }

}

func (ns *NameServer) acceptDN() {
}

func (ns *NameServer) heartBeats() {
}



