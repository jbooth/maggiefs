// package with dependencies on all packages, used to boot up testing and prod instances of services
package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"github.com/jbooth/maggiefs/nameserver"
	"net/rpc"
	"os"
)

type SingleNodeCluster struct {
	leaseServer *leaseserver.LeaseServer
	nameServer  *nameserver.NameServer
	names       maggiefs.NameService
	dataNodes   []*dataserver.DataServer
	datas       maggiefs.DataService
}

func (snc *SingleNodeCluster) Start() error {
	// all services started at construction time for interdependencies
	return nil
}

func (snc *SingleNodeCluster) Close() {
	snc.nameServer.Close()
	snc.leaseServer.Close()
	for _,dn := range snc.dataNodes {
		dn.Close()
	}
}

func (snc *SingleNodeCluster) WaitClosed() error {
	err := snc.nameServer.WaitClosed()
	if err != nil { return err }
	err = snc.leaseServer.WaitClosed()
	if err != nil { return err }
	for _,dn := range snc.dataNodes {
		err = dn.WaitClosed()
		if err != nil { return err }
	}
	return nil
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
func NewNameServer(cfg *NNConfig, format bool) (*NameLeaseServer, error) {
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
	nls.nameserver, err = nameserver.NewNameServer(leaseService, cfg.NameBindAddr, cfg.NNHomeDir, cfg.ReplicationFactor, format)
	if err != nil {
		fmt.Printf("Error creating nameserver: %s\n\n Nameserver config: %+v\n", err.Error(), cfg)
		return nls, err
	}
	nls.nameserver.Start()
	return nls, err
}

func NewDataServer(cfg *DSConfig) (*dataserver.DataServer, error) {
	nameService, err := NewNameClient(cfg.NameAddr)
	if err != nil {
		return nil, err
	}
	return dataserver.NewDataServer(cfg.VolumeRoots, cfg.DataClientBindAddr, cfg.NameDataBindAddr, nameService)
}

// bindIn
func NewSingleNodeCluster(volRoots [][]string, nameHome string, bindHost string, startPort int, replicationFactor uint32, format bool) (*SingleNodeCluster, error) {
	var err error
	cl := &SingleNodeCluster{}
	// start leaseserver and nameserver

	nncfg := &NNConfig{}
	nncfg.LeaseBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	startPort++
	nncfg.NameBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
	startPort++
	nncfg.NNHomeDir = nameHome
	nncfg.ReplicationFactor = replicationFactor

	nls, err := NewNameServer(nncfg, format)
	if err != nil {
		return nil, err
	}
	cl.leaseServer = nls.leaseServer
	cl.nameServer = nls.nameserver
	cl.names, err = NewNameClient(nncfg.NameBindAddr)
	if err != nil {
		return cl, err
	}
	// start dataservers
	cl.dataNodes = make([]*dataserver.DataServer, len(volRoots))
	for idx, dnVolRoots := range volRoots {
		dataClientAddr := fmt.Sprintf("%s:%d", bindHost, startPort)
		startPort++
		nameDataAddr := fmt.Sprintf("%s:%d", bindHost, startPort)
		startPort++
		cl.dataNodes[idx], err = dataserver.NewDataServer(dnVolRoots, dataClientAddr, nameDataAddr, cl.names)
		if err != nil {
			return cl, err
		}
	}
	return cl, nil
}

func TestCluster(numDNs int, volsPerDn int, replicationFactor uint32, baseDir string) (*SingleNodeCluster, error) {
	err := os.Mkdir(baseDir, 0777)
	if err != nil {
		return nil, err
	}
	nameBase := baseDir + "/name"
	err = os.Mkdir(nameBase, 0777)
	if err != nil {
		return nil, err
	}

	dataBase := baseDir + "/data"
	err = os.Mkdir(dataBase, 0777)
	if err != nil {
		return nil, nil
	}
	volRoots := make([][]string, numDNs)
	for i := 0; i < numDNs; i++ {
		dnBase := fmt.Sprintf("%s/dn%d", dataBase, i)
		err = os.Mkdir(dnBase, 0777)
		if err != nil {
			return nil, fmt.Errorf("TestCluster: Error trying to create dn base dir %s : %s\n", dnBase, err.Error())
		}
		dnRoots := make([]string, volsPerDn)
		for j := 0; j < volsPerDn; j++ {
			dnRoots[j] = fmt.Sprintf("%s/vol%d", dnBase, j)
			err = os.Mkdir(dnRoots[j], 0777)
			if err != nil {
				return nil, fmt.Errorf("TestCluster: Error trying to create dir %s : %s\n", dnRoots[j], err.Error())
			}
		}
		volRoots[i] = dnRoots
	}
	return NewSingleNodeCluster(volRoots, nameBase, "0.0.0.0", 11001, replicationFactor, true)
}
