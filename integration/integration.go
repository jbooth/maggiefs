
// package with dependencies on all packages, used to boot up testing and prod instances of services
package integration

import (
  "github.com/jbooth/maggiefs/nameserver"
  "github.com/jbooth/maggiefs/leaseserver"
  "github.com/jbooth/maggiefs/maggiefs"
  "github.com/jbooth/maggiefs/dataserver"
  "os"
  "fmt"
  "net/rpc"
)

type SingleNodeCluster struct {
  leaseServer *leaseserver.LeaseServer
  leases maggiefs.LeaseService
  nameServer *nameserver.NameServer
  names maggiefs.NameService
  dataNodes []*dataserver.DataServer
  datas maggiefs.DataService
}

type NameLeaseServer struct {
	leaseServer *leaseserver.LeaseServer
	nameserver *nameserver.NameServer
}



func NewNameClient(addr string) (maggiefs.NameService,error) {
  client, err := rpc.Dial("tcp", addr)
  if err != nil {
    return nil,err
  }
  return maggiefs.NewNameServiceClient(client),nil
}

func NewNameServer(cfg *NNConfig, format bool) (nls *NameLeaseServer, err error) {
	nls.leaseServer,err = leaseserver.NewLeaseServer(cfg.LeaseBindAddr)
	if err != nil {
		return
	}
	leaseService,err := leaseserver.NewLeaseClient(cfg.LeaseBindAddr)
	if err != nil {
		return
	}
	nls.nameserver,err = nameserver.NewNameServer(leaseService,cfg.NameBindAddr, cfg.NNHomeDir, cfg.ReplicationFactor,format)
	return
}

func NewDataServer(cfg *DSConfig) (*dataserver.DataServer, error) {
	nameService,err := NewNameClient(cfg.NameAddr)
	if err != nil {
	  return nil,err
	}
	return dataserver.NewDataServer(cfg.VolumeRoots,cfg.DataClientBindAddr,cfg.NameDataBindAddr,nameService)
}


// bindIn
func NewSingleNodeCluster(volRoots [][]string, nameHome string, bindHost string, startPort int, replicationFactor uint32, format bool) (cl *SingleNodeCluster,err error) {
  // start leaseserver and nameserver
  leaseAddr := fmt.Sprintf("%s:%d",bindHost,startPort)
  startPort++
  cl.leaseServer,err = leaseserver.NewLeaseServer(leaseAddr)
  if err != nil {
  	return cl,err
  }
  cl.leases,err = leaseserver.NewLeaseClient(leaseAddr)
  if err != nil {
  	return cl,err
  }
  nameAddr := fmt.Sprintf("%s:%d",bindHost,startPort)
  startPort++
  cl.nameServer,err = nameserver.NewNameServer(cl.leases,nameAddr,nameHome,replicationFactor,format)
  if err != nil {
  	return cl,err
  }
  namesClient,err := rpc.Dial("tcp4",nameAddr)
  if err != nil {
  	return cl,err
  }
  cl.names = maggiefs.NewNameServiceClient(namesClient)
  // start dataservers
  cl.dataNodes = make([]*dataserver.DataServer, len(volRoots))
  for idx,dnVolRoots := range volRoots {
  	dataClientAddr := fmt.Sprintf("%s:%d",bindHost,startPort)
  	startPort++
  	nameDataAddr := fmt.Sprintf("%s:%d",bindHost,startPort)
  	startPort++
  	cl.dataNodes[idx],err = dataserver.NewDataServer(dnVolRoots,dataClientAddr,nameDataAddr,cl.names)
  	if err != nil {
  		return cl,err
  	}
  }
  return cl,nil
} 

func TestCluster(numDNs int, baseDir string) (*nameserver.NameServer,[]*dataserver.DataServer,error) {
  err := os.Mkdir(baseDir,0777)
  if err != nil {
    return nil,nil,err
  }
  nameBase := baseDir + "/name"
  err = os.Mkdir(nameBase,0777)
  if err != nil {
    return nil,nil,err
  }
  if err != nil {
    return nil,nil,nil
  }
  for i := 0 ; i < numDNs ; i++ {
    
  }
  return nil,nil,nil
} 



