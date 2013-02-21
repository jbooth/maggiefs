package dataserver

import (
  "net"
  "github.com/jbooth/maggiefs/maggiefs"
)

type DataServer struct {
  volumes map[int32] *volume
  unformatted []string
  // accepts data conns for read/write requests
  dataIface *net.TCPListener
  // accepts conn from namenode 
  nameDataIface *net.TCPListener
}

func NewDataServer(config *DSConfig) {
  // scan volumes
  for _,volRoot := range config.volumeRoots {
    
  }
  
  // start service
}
// startup

// NameDataIFace methods
func (ds *DataServer) serveNameData() {

}

func (ds *DataServer) HeartBeat() (maggiefs.DataNodeStat,error) {
  
}

func (ds *DataServer) Format(volName string, volId int32) (*maggiefs.VolumeStat,error) {
  return nil,nil
}

//  HeartBeat() (DataNodeStat, error)
//  
//  Format(volId int32) (VolumeStat, error)
//  AddBlock(blk Block, volId int32) error
//  RmBlock(id uint64, volId int32) error
//  ExtendBlock(blk Block, volId int32) error  
//  
//  BlockReport(volId int32) ([]Block,error)

func (ds *DataServer) {
}
// read/write methods

