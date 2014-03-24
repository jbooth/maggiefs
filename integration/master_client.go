package integration

import (
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"log"
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
	masterTCPAddr, err := net.ResolveTCPAddr("tcp", masterAddr)
	if err != nil {
		return nil, err
	}
	log.Printf("Dialing for leaseServer on %s : %d", masterTCPAddr, SERVNO_LEASESERVER)
	leaseConn, err := mrpc.DialHandler(masterTCPAddr, SERVNO_LEASESERVER)
	if err != nil {
		return nil, err
	}
	log.Printf("Wrapping leaseClient around conn %s", leaseConn)
	ret.Leases, err = leaseserver.NewLeaseClient(leaseConn)
	if err != nil {
		return ret, err
	}
	log.Printf("Dialing RPC for nameserver at masterAddr %s", masterTCPAddr)
	nameClient, err := mrpc.DialRPC(masterTCPAddr)
	if err != nil {
		return ret, nil
	}
	ret.Names = maggiefs.NewNameServiceClient(nameClient)
	ret.Datas, err = dataserver.NewDataClient(ret.Names)
	return ret, err
}
