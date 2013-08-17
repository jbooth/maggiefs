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
func NewNameServer(cfg *conf.NSConfig, format bool) (*NameLeaseServer, error) {
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
func NewSingleNodeCluster(nncfg *conf.NSConfig, ds []*conf.DSConfig, format bool) (*SingleNodeCluster, error) {
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

