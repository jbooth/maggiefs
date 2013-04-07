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
  client,err := rpc.Dial("tcp",fmt.Sprintf("%s:%d",config.NameHost, config.NamePort))
  
  if err != nil { return nil,err }
  return NewDataServer2(config,maggiefs.NewNameServiceClient(client))
}

// create a new dataserver by joining the specified nameservice
func NewDataServer2(config *DSConfig, ns maggiefs.NameService) (ds *DataServer,err error) {
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
  if dnInfo.DnId == 0 {
    dnInfo.DnId,err = ns.NextDnId()
  }
  dnInfo.Addr = config.DataClientBindAddr
  
  // format unformatted volumes
  for _,path := range unformatted {
      volId,err := ns.NextVolId()
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
  
  ds = &DataServer{ns, dnInfo, volumes,dataClientListen,nameDataListen}
  // start servicing namedata
  nameData := maggiefs.NewNameDataIfaceService(ds)
  server := rpc.NewServer()
  server.Register(nameData)
  go server.Accept(nameDataListen)
  // start servicing client data
  go ds.serveClientData() 
  // register ourselves with namenode, namenode will query us for volumes
  err = ns.Join(dnInfo.DnId, config.NameDataBindAddr)
  return ds,nil
}

func (ds *DataServer) serveNameData() {
  
}

func (ds *DataServer) serveClientData() {
}


func (ds *DataServer) HeartBeat() (stat *maggiefs.DataNodeStat, err error) {
  ret := maggiefs.DataNodeStat { ds.info, make([]maggiefs.VolumeStat, len(ds.volumes), len(ds.volumes)) }
  idx := 0
  for _,vol := range ds.volumes {
    ret.Volumes[idx],err = vol.HeartBeat()
    if err != nil { return nil,err }
    idx++
  }
  return &ret,nil
}

func (ds *DataServer) AddBlock(blk maggiefs.Block, volId int32) (err error) {
  vol,exists := ds.volumes[volId]
  if ! exists { return fmt.Errorf("No volume for volID %d",volId) }
  return vol.AddBlock(blk)
}

func (ds *DataServer) RmBlock(id uint64, volId int32) (err error) {
  vol,exists := ds.volumes[volId]
  if ! exists { return fmt.Errorf("No volume for volID %d",volId) }
  return vol.RmBlock(id)
}

func (ds *DataServer) TruncBlock(blk maggiefs.Block, volId int32, newSize uint32) ( err error ) {
  vol,exists := ds.volumes[volId]
  if ! exists { return fmt.Errorf("No volume for volID %d",volId) }
  return vol.TruncBlock(blk,newSize)
}

func (ds *DataServer) BlockReport(volId int32) (blocks []maggiefs.Block, err error) {
  vol,exists := ds.volumes[volId]
  if ! exists { return nil,fmt.Errorf("No volume for volID %d",volId) }
  return vol.BlockReport()
}

// read/write methods

