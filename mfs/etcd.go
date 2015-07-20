package main

import (
	"github.com/coreos/go-etcd/etcd"
	"github.com/jbooth/maggiefs/integration"
)

// etcd host and me should both be in the "X.X.X.X:P" format
func serviceFor(etcdHost string, me string) integration.Service {
	client := etcd.NewClient(machines)
	masterNode := "/mfs/master"
	// check master node

	// if populated and UP, join as peer

	// if populated and BOOTING, wait till UP then join as peer

	// if INIT, call  leader election and start

}

// the value of leader node is one of:  "INIT", "X.X.X.X:P:ELECTED", "X.X.X.X:P:UP"
// INIT means nobody elected, host:port:ELECTED when a leader is coming online
// and host:port:UP when safe to connect to leader.

// note that if master crashes coming up, we don't elect a new one or have any failover strategy
// we'll implement that once we actually have a failover design

func (client *etcd.Client) electLeader() string {

}
