// package with dependencies on all packages, used to boot up testing and prod instances of services
package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"github.com/jbooth/maggiefs/nameserver"
  "github.com/jbooth/maggiefs/conf"
	"net/rpc"
	"os"
)

type SingleNodeCluster struct {
	LeaseServer *leaseserver.LeaseServer
	Leases      maggiefs.LeaseService
	NameServer  *nameserver.NameServer
	Names       maggiefs.NameService
	DataNodes   []*dataserver.DataServer
	Datas       maggiefs.DataService
}

func (snc *SingleNodeCluster) Start() error {
	// all services started at construction time for interdependencies
	return nil
}

func (snc *SingleNodeCluster) Close() {
	snc.NameServer.Close()
	snc.LeaseServer.Close()
	for _, dn := range snc.DataNodes {
		dn.Close()
	}
}

func (snc *SingleNodeCluster) WaitClosed() error {
	err := snc.NameServer.WaitClosed()
	if err != nil {
		return err
	}
	err = snc.LeaseServer.WaitClosed()
	if err != nil {
		return err
	}
	for _, dn := range snc.DataNodes {
		err = dn.WaitClosed()
		if err != nil {
			return err
		}
	}
	return nil
}

type Client struct {
	Leases maggiefs.LeaseService
	Names  maggiefs.NameService
	Datas  *dataserver.DataClient // TODO generalize this to maggiefs.DataService
}

func NewClient(nameAddr string, leaseAddr string, connsPerDn int) (*Client, error) {
	ret := &Client{}
	var err error
	ret.Leases, err = leaseserver.NewLeaseClient(leaseAddr)
	if err != nil {
		return ret, err
	}
	ret.Names, err = NewNameClient(nameAddr)
	if err != nil {
		return ret, nil
	}
	ret.Datas, err = dataserver.NewDataClient(ret.Names, connsPerDn)
	return ret, err
}

type NameLeaseServer struct {
	leaseServer *leaseserver.LeaseServer
	nameserver  *nameserver.NameServer
}

// no-op, we are started at construction time
func (n *NameLeaseServer) Start() {
}

func (n *NameLeaseServer) Close() error {
	retErr := n.leaseServer.Close()
	err := n.nameserver.Close()
	if err != nil {
		return err
	}
	return retErr
}

func (n *NameLeaseServer) WaitClosed() error {
	retErr := n.leaseServer.WaitClosed()
	err := n.nameserver.WaitClosed()
	if err != nil {
		return err
	}
	return retErr
}

func NewNameClient(addr string) (maggiefs.NameService, error) {
	fmt.Printf("nameclient dialing %d for rpc\n", addr)
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("Error dialing nameclient tcp to %s : %s", addr, err.Error())
	}
	return mrpc.NewNameServiceClient(client), nil
}

// returns a started nameserver -- we must start lease server in order to boot up nameserver, so
func NewNameServer(cfg *conf.NNConfig, format bool) (*NameLeaseServer, error) {
	nls := &NameLeaseServer{}
	var err error = nil
	fmt.Println("creating lease server")
	nls.leaseServer, err = leaseserver.NewLeaseServer(cfg.LeaseBindAddr)
	if err != nil {
		return nls, err
	}
	nls.leaseServer.Start()
	fmt.Println("creating lease client")
	leaseService, err := leaseserver.NewLeaseClient(cfg.LeaseBindAddr)
	if err != nil {
		return nls, err
	}
	fmt.Println("creating name server")
	nls.nameserver, err = nameserver.NewNameServer(leaseService, cfg.NameBindAddr, cfg.WebBindAddr, cfg.NNHomeDir, cfg.ReplicationFactor, format)
	if err != nil {
		fmt.Printf("Error creating nameserver: %s\n\n Nameserver config: %+v\n", err.Error(), cfg)
		return nls, err
	}
	nls.nameserver.Start()
	return nls, err
}

//func NewDataServer(cfg *DSConfig) (*dataserver.DataServer, error) {
//	nameService, err := NewNameClient(cfg.NameAddr)
//	if err != nil {
//		return nil, err
//	}
//	return dataserver.NewDataServer(cfg.VolumeRoots, cfg.DataClientBindAddr, cfg.NameDataBindAddr, nameService)
//}

// TODO refactor to use NewConfSet
func NewSingleNodeCluster(nncfg *conf.NNConfig, ds []*conf.DSConfig, format bool) (*SingleNodeCluster, error) {
	cl := &SingleNodeCluster{}

	nls, err := NewNameServer(nncfg, format)
	if err != nil {
		return nil, err
	}
	cl.LeaseServer = nls.leaseServer
	cl.NameServer = nls.nameserver
	fmt.Println("Starting name client")
	cl.Names, err = NewNameClient(nncfg.NameBindAddr)
	if err != nil {
	 return cl,err
	}
	fmt.Println("starting lease client")
	cl.Leases, err = leaseserver.NewLeaseClient(nncfg.LeaseBindAddr)
	 if err != nil {
    return cl, err
  }
  // start data client
  dc, err := dataserver.NewDataClient(cl.Names, 1)
  if err != nil {
    return cl, fmt.Errorf("error building dataclient : %s", err.Error())
  }
  cl.Datas = dc
  // start dataservers
  cl.DataNodes = make([]*dataserver.DataServer, len(ds))
	for idx,dscfg := range ds {
	 fmt.Println("Starting DS with cfg %+v\n",dscfg)
	 cl.DataNodes[idx],err = dataserver.NewDataServer(dscfg.VolumeRoots,dscfg.DataClientBindAddr,dscfg.NameDataBindAddr, dscfg.WebBindAddr, cl.Names,dc)
	 if err != nil {
	   return cl,err
	 }
	}
	return cl,nil
}

func NewConfSet(volRoots [][]string, nameHome string, bindHost string, startPort int, replicationFactor uint32, format bool) (*conf.NNConfig, []*conf.DSConfig) {
	nncfg := &conf.NNConfig{}
	nncfg.LeaseBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	startPort++
	nncfg.NameBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	startPort++
	nncfg.WebBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	startPort++
	nncfg.NNHomeDir = nameHome
	nncfg.ReplicationFactor = replicationFactor
	dscfg := make([]*conf.DSConfig, len(volRoots))
	for idx, dnVolRoots := range volRoots {
		thisDscfg := &conf.DSConfig{}
		thisDscfg.DataClientBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
		startPort++
		thisDscfg.NameDataBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
		startPort++
		thisDscfg.WebBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
		startPort++
		thisDscfg.VolumeRoots = dnVolRoots
		thisDscfg.LeaseAddr = nncfg.LeaseBindAddr
		thisDscfg.NameAddr = nncfg.NameBindAddr
		dscfg[idx] = thisDscfg

	}
	return nncfg, dscfg
}

func NewConfSet2(numDNs int, volsPerDn int, replicationFactor uint32, baseDir string) (*NNConfig, []*DSConfig, error) {
	err := os.Mkdir(baseDir, 0777)
	if err != nil {
		return nil, nil, err
	}
	nameBase := baseDir + "/name"
	err = os.Mkdir(nameBase, 0777)
	if err != nil {
		return nil, nil, err
	}

	dataBase := baseDir + "/data"
	err = os.Mkdir(dataBase, 0777)
	if err != nil {
		return nil, nil, err
	}
	volRoots := make([][]string, numDNs)
	for i := 0; i < numDNs; i++ {
		dnBase := fmt.Sprintf("%s/dn%d", dataBase, i)
		err = os.Mkdir(dnBase, 0777)
		if err != nil {
			return nil, nil, fmt.Errorf("TestCluster: Error trying to create dn base dir %s : %s\n", dnBase, err.Error())
		}
		dnRoots := make([]string, volsPerDn)
		for j := 0; j < volsPerDn; j++ {
			dnRoots[j] = fmt.Sprintf("%s/vol%d", dnBase, j)
			err = os.Mkdir(dnRoots[j], 0777)
			if err != nil {
				return nil, nil, fmt.Errorf("TestCluster: Error trying to create dir %s : %s\n", dnRoots[j], err.Error())
			}
		}
		volRoots[i] = dnRoots
	}
	nnc, dsc := NewConfSet(volRoots, nameBase, "0.0.0.0", 11001, replicationFactor, true)
	return nnc, dsc, nil
}
