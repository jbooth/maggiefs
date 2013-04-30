
// package with dependencies on all packages, used to boot up testing and prod instances of services
package integration

import (
  "github.com/jbooth/maggiefs/nameserver"
  "github.com/jbooth/maggiefs/leaseserver"
  "github.com/jbooth/maggiefs/maggiefs"
  "github.com/jbooth/maggiefs/dataserver"
  "os"
)

type SingleNodeCluster struct {
  leaseServer *leaseserver.LeaseServer
  leases maggiefs.LeaseService
  nameServer *nameserver.NameServer
  names maggiefs.NameService
  dataNodes []*dataserver.DataServer
  datas maggiefs.DataService
}

func NewSingleNodeCluster(volRoots [][]string, nameHome string, bindInterface string, startPort int) (*SingleNodeCluster,error) {
  
  return nil,nil
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
  leaseAddr := "0.0.0.0:10100"
  nameAddr := "0.0.0.0:10200"
  nameServer,err := nameserver.NewNameServer(leaseAddr,nameAddr,nameBase,true)
  if err != nil {
    return nil,nil,nil
  }
  for i := 0 ; i < numDNs ; i++ {
    
  }
  return nameServer,nil,nil
} 



