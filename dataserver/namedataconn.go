package dataserver

import (
  "sync"
  "encoding/gob"
  "net"
  "github.com/jbooth/maggiefs/maggiefs"
)

// represents stateful connection from datanode->namenode
// datanode connects to namenode on startup, and from here, the namenode 
// can issue commands to the datanode

// implements maggiefs.NameDataIFace from namenode side

type NameDataConn struct {
  l *sync.Mutex
  e *gob.Encoder
  d *gob.Decoder
  c *net.TCPConn 
}

//  HeartBeat() (DataNodeStat, error)
//  
//  Format(volId int32) (VolumeStat, error)
//  AddBlock(id uint64) error
//  RmBlock(id uint64) error
//  ExtendBlock() error  
//  
//  BlockReport() ([]Block,error)

const (
  OP_HEARTBEAT = iota
  OP_FORMAT = iota
  OP_ADDBLOCK = iota
  OP_RMBLOCK = iota
  OP_EXTENDBLOCK = iota
  OP_BLKREPORT = iota
)

func (nd *NameDataConn) HeartBeat() (maggiefs.DataNodeStat, error) {
  return maggiefs.DataNodeStat{},nil
}

func (nd *NameDataConn) AddBlock(id uint64) error {
  return nil
}





