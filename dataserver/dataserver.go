package dataserver

import (
  "net"
  "github.com/jbooth/maggiefs/maggiefs"
)

type DataServer struct {
  info maggiefs.DataNodeInfo
  // live and unformatted volumes
  volumes map[int32] *volume
  unformatted []string
  // accepts data conns for read/write requests
  dataIface *net.TCPListener
  // accepts conn from namenode 
  nameDataIface *net.TCPListener
}

func NewDataServer(config *DSConfig) (*DataServer,error) {
  // scan volumes
  volumes := make(map[int32] *volume)
  unformatted := make([]string, 0)
  for _,volRoot := range config.volumeRoots {
    if validVolume(volRoot) {
      // initialize volume
      vol,err := loadVolume(volRoot)
      if err != nil { return nil,err }
      volumes[vol.id] = vol    
    } else {
      // mark unformatted, wait for order from namenode to format
      unformatted = append(unformatted,volRoot)
    }
  }
  // start up listeners
  dataClientBindAddr := net.ResolveTCPAddr("tcp4",DSConfig.dataClientBindAddr) 
  nameDataBindAddr := net.ResolveTCPAddr("tcp4",DSConfig.nameDataBindAddr)
  
  // start servicing
  ds := &DataServer{volumes,unformatted,dataClientBindAddr,nameDataBindAddr}
  
  return ds,nil
}
// startup

func (ds *DataServer) serveNameData() {

}

func (ds *DataServer) HeartBeat() (*maggiefs.DataNodeStat,error) {
  return nil,nil
}

func (ds *DataServer) Format(volName string, volId int32) (*maggiefs.VolumeStat,error) {
  return nil,nil
}

// NameDataIFace methods
//  HeartBeat() (DataNodeStat, error)
//  
//  Format(volId int32) (VolumeStat, error)
//  AddBlock(blk Block, volId int32) error
//  RmBlock(id uint64, volId int32) error
//  ExtendBlock(blk Block, volId int32) error  
//  
//  BlockReport(volId int32) ([]Block,error)

// read/write methods

