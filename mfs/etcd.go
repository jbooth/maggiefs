package main

import (
	"github.com/coreos/go-etcd/etcd"
	"github.com/jbooth/maggiefs/integration"
	"strings"
)

// etcd host and me should both be in the "X.X.X.X:P" format
func serviceFor(etcdHost string, me string, dataDir string) (integration.Service, error) {
	client := etcd.NewClient(machines)
	defer client.Close()
	masterNodeLoc := "/mfs/master/current"
	// check master node
	masterGetResp, err := client.get(masterNodeLoc, false, false)
	if err != nil {
		return nil, err
	}
	masterNode := masterGetResp.Node

	// the value of masterNode is one of:  "INIT", "X.X.X.X:P:ELECTED", "X.X.X.X:P:UP"
	// INIT means nobody elected, host:port:ELECTED when a leader is coming online
	// and host:port:UP when safe to connect to leader.
	if masterNode.Value == "INIT" {
		// if INIT, call  leader election and serve either master or peer
		return leadOrJoin(client, me, dataDir)
	}
	if strings.Count(masterNode.Value, ":") != 2 {
		return nil, fmt.Errorf("Should have either INIT or a host:port:status tuple in etcd node %s -- had %s", masterNodeLoc, masterNode.Value)
	}

	// if populated and BOOTING, wait till UP then join as peer

}

// note that if master crashes coming up, we don't elect a new one or have any failover strategy
// we'll implement that once we actually have a failover design

func leadOrJoin(client *etcd.Client, me string, dataDir string) (integration.Service, error) {

}

func serveMaster(client *etcd.Client, me string, dataDir string) (integration.Service, error) {
}

// first arg is last known val for master
func servePeer(masterNode etcd.Node, client *etcd.Client, me string, dataDir string) {
	hostPortStatus = strings.Split(masterNode.Value, ":")
	host := hostPortStatus[0]
	port := hostPortStatus[1]
	stat := hostPortStatus[2]
	if stat == "UP" {
		// if populated and UP, join as peer
	} else {
		// will iterate and back off
	}

}
