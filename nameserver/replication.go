package nameserver

import (
  "net"
  "sync"
  "github.com/jbooth/maggiefs/maggiefs"
  "sort"
  "fmt"
)

type datanodeStat struct {
  conn maggiefs.NameDataIface
  stat maggiefs.DataNodeStat
  l    *sync.Mutex
}

type volumeInfo struct {
  id uint32
  hostid uint32
  size uint64
  used uint64
  free uint64
}

type hostInfo struct {
  id uint32
  addr *net.TCPAddr
  volumes []volumeInfo
  conn maggiefs.NameDataIface
  l *sync.Mutex
}



type replicationManager struct {
  volumes map[uint32] volumeInfo
  hosts map[uint32] hostInfo
  
}


func (rm *replicationManager) addBlock(inodeid uint64) (maggiefs.Block,error) {
  
}

func (rm *replicationManager) extendBlock(blockid uint64, delta uint32) error {

}

// returns a slice of hosts for a new block, should be ns.replicationFactor in length
// finds N with most space
// if suggested > 0, will include suggested
func (ns *NameServer) hostsForNewBlock(suggested *uint32) ([]uint32,error) {
  ns.dnLock.Lock()
  defer ns.dnLock.Unlock()
  if ns.replicationFactor > len(ns.dataNodes) {
    return []uint32{},fmt.Errorf("Replication factor %d greater than number of connected data nodes %d : %+v",ns.replicationFactor,len(ns.dataNodes),ns.dataNodes)
  }
  // build slice
  var freeSizes dnFreeSlice = make([]dnFreeSize,len(ns.dataNodes))
  i := 0
  for id,dn := range ns.dataNodes {
    freeSizes[i] = dnFreeSize {id,dn.stat.TotalBytes - dn.stat.BytesUsed }
  }
  // sort
  sort.Sort(freeSizes)
  // return first N
  ret := make([]uint32,ns.replicationFactor)
  i = 0
  if (suggested != nil) { 
    ret[0] = *suggested
    i++
  }
  for ; i < ns.replicationFactor ; i++ {
    ret[i] = freeSizes[i].dn
  }
  // todo should provisionally += the affected datanodes so we don't thundering-herd them
  return ret,nil
}

type dnFreeSize struct {
  dn uint32
  freeSpace uint64
}

type dnFreeSlice []dnFreeSize
func (s dnFreeSlice) Swap(i,j int) { s[i],s[j] = s[j],s[i]}
func (s dnFreeSlice) Len() int  { return len(s) } 
func (s dnFreeSlice) Less(i,j int) bool { return s[i].freeSpace < s[j].freeSpace }

