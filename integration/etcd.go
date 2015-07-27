package main

import (
	"github.com/coreos/go-etcd/etcd"
	"strings"
)

//core@core-01 ~ $ ./etcdcl
//respGet, err: <nil>,100: Key not found (/test) [283]respCreate,err: &{Action:create Node:0xc208070840 PrevNode:<nil> EtcdIndex:284 RaftIndex:1268 RaftTerm:0},%!s(<nil>)respCreate,err: <nil>,105: Key already exists (/test) [284]respGet,err%!(EXTRA *etcd.Response=&{get 0xc208070c60 <nil> 284 1269 0}, <nil>)

// etcd host and me should both be in the "X.X.X.X:P" format
func serviceFor(etcdHost string, me string, dataDir string) (Service, error) {
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

}

// note that if master crashes coming up, we don't elect a new one or have any failover strategy
// we'll implement that once we actually have a failover design

func leadOrJoin(client *etcd.Client, myAddr string, dataDir string, mountPoint string) (Service, error) {
	// try to set as us:ELECTED
	createResp, err := client.Create("/mfs/masterAddr", myAddr+":ELECTED", 1e12)
	if createResp != nil && err == nil {
		// we elected ourselves as leader, start master
		mconf := integration.MasterConfig{
			BindAddr:          myAddr,
			NameHome:          dataDir,
			ReplicationFactor: 2,
		}
		return serveMaster(client, myAddr, dataDir)
	} else {
		getResp, err := client.Get("/mfs/masterAddr", false, false)
		if err != nil {
			return nil, fmt.Errorf("Error getting master addr: %s", err)
		}
		if getResp == nil || getResp.Node == nil {
			return nil, fmt.Errorf("Should never happen, resp was nil on successful get of /mfs/masterAddr")
		}
		return servePeer(masterNode, client, me, dataDir)
	}
	// if unsuccessful, switch to join as peer
	// if successful,

}

// assumes master node currently set to myAddr:ELECTED
func serveMaster(client *etcd.Client, myAddr string, dataDir string, mountPoint string) (Service, error) {
	// bootstrap leader
	// set up us:UP

}

// first arg is last known val for master
func servePeer(masterNode etcd.Node, client *etcd.Client, me string, dataDir string, mountPoint string) {
	hostPortStatus = strings.Split(masterNode.Value, ":")
	host := hostPortStatus[0]
	port := hostPortStatus[1]
	stat := hostPortStatus[2]
	if stat == "ELECTED" {
		// keep checking until we're UP
	}
	if stat == "UP" {
		// if populated and UP, join as peer
	} else {
		// will iterate until up, or return error
	}
}
