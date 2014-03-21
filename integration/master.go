package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/mrpc"
	"github.com/jbooth/maggiefs/nameserver"
	"net"
	"time"
)

const (
	SERVNO_LEASESERVER = uint32(1)
)

var typeCheck mrpc.Service = &NameLeaseServer{}

type NameLeaseServer struct {
	leaseServer *leaseserver.LeaseServer
	nameserver  *nameserver.NameServer
	serv        mrpc.Service
}

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
	nls := &NameLeaseServer{}
	var err error = nil
	fmt.Println("creating lease server")
	nls.leaseServer = leaseserver.NewLeaseServer()
	fmt.Println("creating name server")
	nls.nameserver, err = nameserver.NewNameServer(cfg.WebBindAddr, cfg.NameHome, cfg.ReplicationFactor, format)
	if err != nil {
		fmt.Printf("Error creating nameserver: %s\n\n Nameserver config: %+v\n", err.Error(), cfg)
		return nls, err
	}
	opMap := make(map[uint32]func(*net.TCPConn))
	opMap[SERVNO_LEASESERVER] = nls.leaseServer.ServeConn
	
	nls.serv,err = mrpc.CloseableRPC(cfg.BindAddr, impl interface{}, customHandlers map[uint32]func(newlyAcceptedConn *net.TCPConn), name string) (*CloseableServer, error) {
	return nls, err
}
