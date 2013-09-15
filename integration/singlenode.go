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
	"os"
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
func NewSingleNodeCluster(numDNs int, volsPerDn int, replicationFactor uint32, baseDir string) (*SingleNodeCluster, error) {
	cl := &SingleNodeCluster{}
	nncfg,ds,err := newConfSet2(numDNs, volsPerDn, replicationFactor, baseDir)
	if err != nil {
		return nil,err
	}
	nls, err := NewNameServer(nncfg, true)
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


// used to bootstrap singlenode clusters
func newConfSet(volRoots [][]string, nameHome string, bindHost string, startPort int, replicationFactor uint32, format bool) (*conf.MasterConfig, []*conf.PeerConfig) {
	nncfg := &conf.MasterConfig{}
	nncfg.LeaseBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	startPort++
	nncfg.NameBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	startPort++
	nncfg.WebBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	startPort++
	nncfg.NameHome = nameHome
	nncfg.ReplicationFactor = replicationFactor
	dscfg := make([]*conf.PeerConfig, len(volRoots))
	for idx, dnVolRoots := range volRoots {
		thisDscfg := &conf.PeerConfig{}
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

// used to bootstrap singlenode clusters
func newConfSet2(numDNs int, volsPerDn int, replicationFactor uint32, baseDir string) (*conf.MasterConfig, []*conf.PeerConfig, error) {
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
	nnc, dsc := newConfSet(volRoots, nameBase, "0.0.0.0", 11001, replicationFactor, true)
	return nnc, dsc, nil
}
