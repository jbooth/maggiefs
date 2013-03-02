package dataserver

import (
  "net"
  "github.com/jbooth/maggiefs/maggiefs"
  "errors"
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
  // get id or format new id
  
  // scan volumes
  volumes := make(map[int32] *volume)
  unformatted := make([]string, 0)
  for _,volRoot := range config.VolumeRoots {
    if validVolume(volRoot) {
      // initialize existing volume
      vol,err := loadVolume(volRoot)
      if err != nil { return nil,err }
      volumes[vol.id] = vol    
    } else {
      // mark unformatted, wait for order from namenode to format
      unformatted = append(unformatted,volRoot)
    }
  }
  // form consensus on host across volumes or error
  var dnInfo maggiefs.DataNodeInfo = maggiefs.DataNodeInfo{}
  for _,vol := range volumes {
    if (dnInfo.DnId != 0) {
      dnInfo = vol.info.DnInfo
    } else {
      // compare new to previous
      if (! dnInfo.Equals(vol.info.DnInfo)) {
        return nil,errors.New("Incompatible dataNodeInfo across volumes!")
      }
    }
  }
  
  
  // start up listeners
  dataClientBindAddr,err := net.ResolveTCPAddr("tcp", config.DataClientBindAddr) 
  if err != nil { return nil,err }
  nameDataBindAddr,err := net.ResolveTCPAddr("tcp",config.NameDataBindAddr)
  if err != nil { return nil,err }
  
  dataClientListen,err := net.ListenTCP("tcp",dataClientBindAddr)
  if err != nil { return nil,err }
  nameDataListen,err := net.ListenTCP("tcp",nameDataBindAddr)
  if err != nil { return nil,err }
  
  // start servicing
  ds := &DataServer{dnInfo, volumes,unformatted,dataClientListen,nameDataListen}
  go ds.serveNameData()
  go ds.serveClientData() 
  return ds,nil
}

func (ds *DataServer) serveNameData() {

}

func (ds *DataServer) serveClientData() {
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

