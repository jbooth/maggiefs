package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/nameserver"
	"github.com/jbooth/maggiefs/mrpc"
	"time"
)

var typeCheck mrpc.Service = &NameLeaseServer{}

type NameLeaseServer struct {
	leaseServer *leaseserver.LeaseServer
	nameserver  *nameserver.NameServer
	serv   mrpc.Service
}

// no-op, we are started at construction time
func (n *NameLeaseServer) Serve() error {
	return n.serv.Serve()
}

func (n *NameLeaseServer) Close() error {
	return n.serv.Close()
}

func (n *NameLeaseServer) WaitClosed() error {
	return n.serv.WaitClosed()
}

// returns a started nameserver -- we must start lease server in order to boot up nameserver, so
func NewNameServer(cfg *conf.MasterConfig, format bool) (*NameLeaseServer, error) {
	multiServ := NewMultiService()
	nls := &NameLeaseServer{}
	nls.serv = multiServ
	var err error = nil
	fmt.Println("creating lease server")
	nls.leaseServer, err = leaseserver.NewLeaseServer(cfg.LeaseBindAddr)
	if err != nil {
		return nls, err
	}
	multiServ.AddService(nls.leaseServer)
	fmt.Println("creating lease client")
	leaseService, err := leaseserver.NewLeaseClient(cfg.LeaseBindAddr)
	if err != nil {
		return nls, err
	}
	fmt.Println("creating name server")
	nls.nameserver, err = nameserver.NewNameServer(leaseService, cfg.NameBindAddr, cfg.WebBindAddr, cfg.NameHome, cfg.ReplicationFactor, format)
	if err != nil {
		fmt.Printf("Error creating nameserver: %s\n\n Nameserver config: %+v\n", err.Error(), cfg)
		return nls, err
	}

	multiServ.AddService(nls.nameserver)
	time.Sleep(time.Second)
	return nls, err
}