// package with dependencies on all packages, used to boot up testing and prod instances of services
package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"github.com/jbooth/maggiefs/nameserver"
	"os"
)

// compile time check
var singleClustCheck mrpc.Service = &SingleNodeCluster{}

// Encapsulates a single node, minus mountpoint.  Used for testing.
type SingleNodeCluster struct {
	LeaseServer *leaseserver.LeaseServer
	Leases      maggiefs.LeaseService
	NameServer  *nameserver.NameServer
	Names       maggiefs.NameService
	DataNodes   []*dataserver.DataServer
	Datas       maggiefs.DataService
	svc         mrpc.Service
}

func (snc *SingleNodeCluster) Serve() error {
	return snc.svc.Serve()
}

func (snc *SingleNodeCluster) Close() error {
	return snc.svc.Close()
}

func (snc *SingleNodeCluster) WaitClosed() error {
	return snc.svc.WaitClosed()
}

func (snc *SingleNodeCluster) HttpAddr() string {
	return "localhost:1103"
}

func NewSingleNodeCluster(numDNs int, volsPerDn int, replicationFactor uint32, baseDir string, mountPoint string, debugMode bool) (*SingleNodeCluster, error) {
	cl := &SingleNodeCluster{}
	nncfg, ds, err := NewConfSet2(numDNs, volsPerDn, replicationFactor, baseDir)
	if err != nil {
		return nil, err
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

	// peer web server hardcoded to 1103
	webServer, err := NewPeerWebServer(cl.Names, cl.Datas, mountPoint, "localhost:1103")
	if err != nil {
		return cl, err
	}
	err = multiServ.AddService(webServer)
	if err != nil {
		return cl, err
	}

	// make service wrapper
	multiServ := NewMultiService()
	cl.svc = multiServ
	multiServ.AddService(nls) // add nameLeaseServer
	multiServ.AddService(webServer)

	cl.Datas = dc
	// start dataservers
	cl.DataNodes = make([]*dataserver.DataServer, len(ds))
	for idx, dscfg := range ds {
		fmt.Println("Starting DS with cfg %+v\n", dscfg)
		// create and register with SingleNodeCluster struct
		ds, err := dataserver.NewDataServer(dscfg.VolumeRoots, dscfg.BindAddr, cl.Names, dc)
		if err != nil {
			return cl, err
		}
		cl.DataNodes[idx] = ds
		// start and register with MultiServ
		opMap := make(map[uint32]func(*net.TCPConn))
		opMap[dataserver.DIAL_READ] = ds.ServeReadConn
		opMap[dataserver.DIAL_WRITE] = ds.ServeWriteConn
		dataServ, err := mrpc.CloseableRPC(cfg.BindAddr, ds, opMap)
		if err != nil {
			return ret, err
		}
		err = multiServ.AddService(dataServ)
		if err != nil {
			return ret, err
		}
	}

	// create mountpoint if necessary
	if mountPoint != "" {
		maggieFuse, err := client.NewMaggieFuse(cl.Leases, cl.Names, cl.Datas, nil)
		if err != nil {
			return cl, err
		}
		mount, err := NewMount(maggieFuse, mountPoint, debugMode)
		if err != nil {
			return cl, err
		}
		err = multiServ.AddService(mount)
		if err != nil {
			return cl, err
		}
	}
	return cl, nil
}

// used to bootstrap singlenode clusters
func NewConfSet(volRoots [][]string, nameHome string, bindHost string, startPort int, replicationFactor uint32, format bool) (*MasterConfig, []*PeerConfig) {
	nncfg := &conf.MasterConfig{}
	nncfg.BindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
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
func NewConfSet2(numDNs int, volsPerDn int, replicationFactor uint32, baseDir string) (*MasterConfig, []*PeerConfig, error) {
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
