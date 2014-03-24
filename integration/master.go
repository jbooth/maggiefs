package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"github.com/jbooth/maggiefs/nameserver"
	"net"
	"strconv"
)

const (
	SERVNO_LEASESERVER = uint32(1)
	SERVNO_PEERJOIN    = uint32(2)
)

var typeCheck Service = &Master{}

type Master struct {
	leaseServer *leaseserver.LeaseServer
	nameserver  *nameserver.NameServer
	serv        *mrpc.CloseableServer
	port        int
}

func (n *Master) Serve() error {
	return n.serv.Serve()
}

func (n *Master) Close() error {
	return n.serv.Close()
}

func (n *Master) WaitClosed() error {
	return n.serv.WaitClosed()
}

// returns a started nameserver -- we must start lease server in order to boot up nameserver, so
func NewMaster(cfg *MasterConfig, format bool) (*Master, error) {
	nls := &Master{}
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

	nls.serv, err = mrpc.CloseableRPC(cfg.BindAddr, "NameService", maggiefs.NewNameServiceService(nls.nameserver), opMap)
	if err != nil {
		return nls, err
	}
	_, port, _ := net.SplitHostPort(cfg.BindAddr)
	nls.port, _ = strconv.Atoi(port)
	return nls, err
}
