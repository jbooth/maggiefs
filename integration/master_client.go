package integration

import (
	"fmt"
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

// takes masterAddr in host:port format
func NewClient(masterAddr string) (*Client, error) {
	ret := &Client{}
	masterTCPAddr, err := net.ResolveTCPAddr(masterAddr)
	if err != nil {
		return nil, err
	}
	leaseConn, err := mrpc.Dial("tcp", masterTCPAddr, SERVNO_LEASESERVER)
	if err != nil {
		return nil, err
	}
	ret.Leases, err = leaseserver.NewLeaseClient(leaseConn)
	if err != nil {
		return ret, err
	}
	nameClient, err := mrpc.DialRPC(masterTCPAddr)
	if err != nil {
		return ret, nil
	}
	ret.Names = mrpc.NewNameServiceClient(nameClient)
	ret.Datas, err = dataserver.NewDataClient(ret.Names, cfg.ConnsPerPeer)
	return ret, err
}
