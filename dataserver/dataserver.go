package dataserver

import (
  "net"
  "net/rpc"
  "github.com/jbooth/maggiefs/maggiefs"
  "errors"
  "fmt"
)

type DataServer struct {
  ns maggiefs.NameService
  info maggiefs.DataNodeInfo
  // live and unformatted volumes
  volumes map[int32] *volume
  // accepts data conns for read/write requests
  dataIface *net.TCPListener
  // accepts conn from namenode 
  nameDataIface *net.TCPListener
}

// create a new dataserver connected to the nameserver specified in config
func NewDataServer(config *DSConfig) (*DataServer,error) {
  client,err := rpc.DialHTTP("tcp",fmt.Sprintf("%s:%d",config.NameHost, config.NamePort))
  
  if err != nil { return nil,err }
  return NewDataServer2(config,maggiefs.NewNameServiceClient(client))
}

// create a new dataserver by joining the specified nameservice
func NewDataServer2(config *DSConfig, ns maggiefs.NameService) (*DataServer,error) {
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
  // format unformatted volumes
  for _,path := range unformatted {
      volId,err := ns.NewVolId()
      if err != nil { return nil,err }
      vol,err := formatVolume(path,maggiefs.VolumeInfo{volId,dnInfo})
      if err != nil { return nil,err }
      volumes[volId] = vol
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
  ds := &DataServer{ns, dnInfo, volumes,dataClientListen,nameDataListen}
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

// NameDataIFace methods
//  // periodic heartbeat with datanode stats so namenode can keep total stats and re-replicate
//  HeartBeat() (stat *DataNodeStat, err error)
//  // add a block to this datanode/volume
//  AddBlock(blk Block, volId int32) (err error)
//  // rm block from this datanode/volume
//  RmBlock(id uint64, volId int32) (err error)
//  // truncate a block
//  TruncBlock(blk Block, volId int32, newSize uint32) (err error)
//  // get the list of all blocks for a volume
//  BlockReport(volId int32) (blocks []Block, err error)

// read/write methods

