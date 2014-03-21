package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"net"
)

// Wrapper for all clients, speaking to the master and to other peers via DataClient
type Client struct {
	Leases maggiefs.LeaseService
	Names  maggiefs.NameService
	Datas  *dataserver.DataClient // TODO generalize this to maggiefs.DataService
}

func NewClient(cfg *conf.PeerConfig) (*Client, error) {
	ret := &Client{}
	masterAddr, err := net.ResolveTCPAddr(cfg.MasterAddr)
	if err != nil {
		return nil, err
	}
	leaseConn, err := mrpc.Dial("tcp", masterAddr, SERVNO_LEASESERVER)
	if err != nil {
		return nil, err
	}
	ret.Leases, err = leaseserver.NewLeaseClient(leaseConn)
	if err != nil {
		return ret, err
	}
	nameClient, err := mrpc.DialRPC(masterAddr)
	if err != nil {
		return ret, nil
	}
	ret.Names = mrpc.NewNameServiceClient(nameClient)
	ret.Datas, err = dataserver.NewDataClient(ret.Names, cfg.ConnsPerPeer)
	return ret, err
}
