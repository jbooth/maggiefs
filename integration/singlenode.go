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
	"log"
	"net"
	"os"
	"time"
)

// compile time check
var singleClustCheck Service = &SingleNodeCluster{}

// Encapsulates a single node, minus mountpoint.  Used for testing.
type SingleNodeCluster struct {
	LeaseServer *leaseserver.LeaseServer
	Leases      maggiefs.LeaseService
	NameServer  *nameserver.NameServer
	Names       maggiefs.NameService
	DataNodes   []*dataserver.DataServer
	Datas       maggiefs.DataService
	svc         Service
}

func (snc *SingleNodeCluster) Serve() error {
	log.Printf("Singlenodecluster starting..")
	return snc.svc.Serve()
}

func (snc *SingleNodeCluster) Close() error {
	return snc.svc.Close()
}

func (snc *SingleNodeCluster) WaitClosed() error {
	return snc.svc.WaitClosed()
}

func (snc *SingleNodeCluster) HttpAddr() string {
	log.Printf("Returning localhost:1103 from SingleNodeCluster.HttpAddr")
	return "localhost:1103"
}

func NewSingleNodeCluster(numDNs int, volsPerDn int, replicationFactor uint32, baseDir string, mountPoint string, debugMode bool) (*SingleNodeCluster, error) {
	cl := &SingleNodeCluster{}
	nncfg, ds, err := NewConfSet2(numDNs, volsPerDn, replicationFactor, baseDir)
	if err != nil {
		return nil, err
	}
	log.Printf("Creating singleNodeCluster with master conf: %s, ds confs: %s", nncfg, ds)
	nls, err := NewMaster(nncfg, true)
	if err != nil {
		return nil, err
	}
	cl.LeaseServer = nls.leaseServer
	cl.NameServer = nls.nameserver

	// make service wrapper
	multiServ := NewMultiService()
	cl.svc = multiServ
	multiServ.AddService(nls) // add nameLeaseServer
	masterAddr := fmt.Sprintf("127.0.0.1:%d", nls.port)
	log.Printf("Connecting client to started master at %s", masterAddr)
	cli, err := NewClient(masterAddr)
	if err != nil {
		return cl, err
	}
	cl.Names = cli.Names
	cl.Leases = cli.Leases
	cl.Datas = cli.Datas

	// peer web server hardcoded to 1103
	webServer, err := NewPeerWebServer(cl.Names, cl.Datas, mountPoint, "localhost:1103")
	if err != nil {
		return cl, err
	}
	err = multiServ.AddService(webServer)
	if err != nil {
		return cl, err
	}

	multiServ.AddService(webServer)
	// start dataservers
	cl.DataNodes = make([]*dataserver.DataServer, len(ds))
	for idx, dscfg := range ds {
		log.Printf("Starting DS with cfg %+v", dscfg)
		// create and register with SingleNodeCluster struct
		ds, err := dataserver.NewDataServer(dscfg.BindAddr, dscfg.VolumeRoots, cl.Names, cl.Datas)
		if err != nil {
			return cl, err
		}
		cl.DataNodes[idx] = ds
		// start and register with MultiServ
		opMap := make(map[uint32]func(*net.TCPConn))
		opMap[dataserver.DIAL_READ] = ds.ServeReadConn
		opMap[dataserver.DIAL_WRITE] = ds.ServeWriteConn
		dataServ, err := mrpc.CloseableRPC(dscfg.BindAddr, "Peer", maggiefs.NewPeerService(ds), opMap)
		if err != nil {
			return cl, err
		}
		err = multiServ.AddService(dataServ)
		if err != nil {
			return cl, err
		}
		dnInfo, _ := ds.HeartBeat()
		err = cl.Names.Join(dnInfo.DnId, dscfg.BindAddr)
		if err != nil {
			return cl, err
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
	time.Sleep(1 * time.Second)
	return cl, nil
}

// used to bootstrap singlenode clusters
func NewConfSet(volRoots [][]string, nameHome string, bindHost string, startPort int, replicationFactor uint32, format bool) (*MasterConfig, []*PeerConfig) {
	nncfg := &MasterConfig{}
	nncfg.BindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	masterAddr := fmt.Sprintf("127.0.0.1:%d", startPort)
	startPort++
	nncfg.WebBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	startPort++
	nncfg.NameHome = nameHome
	nncfg.ReplicationFactor = replicationFactor
	dscfg := make([]*PeerConfig, len(volRoots))
	for idx, dnVolRoots := range volRoots {
		thisDscfg := &PeerConfig{}
		thisDscfg.MasterAddr = masterAddr
		thisDscfg.BindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
		startPort++
		thisDscfg.WebBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
		startPort++
		thisDscfg.VolumeRoots = dnVolRoots
		thisDscfg.MasterAddr = nncfg.BindAddr
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
	nnc, dsc := NewConfSet(volRoots, nameBase, "127.0.0.1", 11004, replicationFactor, true)
	return nnc, dsc, nil
}
