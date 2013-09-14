// package with dependencies on all packages, used to boot up testing and prod instances of services
package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/nameserver"
	"github.com/jbooth/go-fuse/fuse"
)

// compile time check for 


// Encapsulates a single node, minus mountpoint.  Used for testing.
type SingleNodeCluster struct {
	LeaseServer *leaseserver.LeaseServer
	Leases      maggiefs.LeaseService
	NameServer  *nameserver.NameServer
	Names       maggiefs.NameService
	DataNodes   []*dataserver.DataServer
	Datas       maggiefs.DataService
	FuseConnector fuse.RawFileSystem
}

func (snc *SingleNodeCluster) Start() error {
	// all services started at construction time for interdependencies
	return nil
}

func (snc *SingleNodeCluster) Close() error {
	snc.NameServer.Close()
	snc.LeaseServer.Close()
	for _, dn := range snc.DataNodes {
		dn.Close()
	}
	return nil
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



//func NewDataServer(cfg *DSConfig) (*dataserver.DataServer, error) {
//	nameService, err := NewNameClient(cfg.NameAddr)
//	if err != nil {
//		return nil, err
//	}
//	return dataserver.NewDataServer(cfg.VolumeRoots, cfg.DataClientBindAddr, cfg.NameDataBindAddr, nameService)
//}

// TODO refactor to use NewConfSet
func NewSingleNodeCluster(nncfg *conf.MasterConfig, ds []*conf.PeerConfig, format bool) (*SingleNodeCluster, error) {
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
		return cl, err
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
	for idx, dscfg := range ds {
		fmt.Println("Starting DS with cfg %+v\n", dscfg)
		cl.DataNodes[idx], err = dataserver.NewDataServer(dscfg.VolumeRoots, dscfg.DataClientBindAddr, dscfg.NameDataBindAddr, dscfg.WebBindAddr, cl.Names, dc)
		if err != nil {
			return cl, err
		}
	}
	cl.FuseConnector,err = client.NewMaggieFuse(cl.Leases,cl.Names,cl.Datas)
	return cl, err
}
