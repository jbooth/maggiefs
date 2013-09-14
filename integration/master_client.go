package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"net/rpc"
)


// Wrapper for all clients, speaking to the master and to other peers via DataClient
type Client struct {
	Leases maggiefs.LeaseService
	Names  maggiefs.NameService
	Datas  *dataserver.DataClient // TODO generalize this to maggiefs.DataService
}

func NewClient(cfg *conf.PeerConfig) (*Client,error) {
	ret := &Client{}
	var err error
	ret.Leases, err = leaseserver.NewLeaseClient(cfg.LeaseAddr)
	if err != nil {
		return ret, err
	}
	ret.Names, err = NewNameClient(cfg.NameAddr)
	if err != nil {
		return ret, nil
	}
	ret.Datas, err = dataserver.NewDataClient(ret.Names, cfg.ConnsPerPeer)
	return ret, err
}

//func NewClient(nameAddr string, leaseAddr string, connsPerDn int) (*Client, error) {
//	ret := &Client{}
//	var err error
//	ret.Leases, err = leaseserver.NewLeaseClient(leaseAddr)
//	if err != nil {
//		return ret, err
//	}
//	ret.Names, err = NewNameClient(nameAddr)
//	if err != nil {
//		return ret, nil
//	}
//	ret.Datas, err = dataserver.NewDataClient(ret.Names, connsPerDn)
//	return ret, err
//}

// TODO move to nameserver pkg
func NewNameClient(addr string) (maggiefs.NameService, error) {
	fmt.Printf("nameclient dialing %d for rpc\n", addr)
	client, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("Error dialing nameclient tcp to %s : %s", addr, err.Error())
	}
	return mrpc.NewNameServiceClient(client), nil
}