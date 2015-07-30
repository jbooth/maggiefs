package main

import (
	"github.com/coreos/go-etcd/etcd"
	etcdembed 	x"github.com/coreos/etcd/integration"
	"strings"
	"time"
)

//core@core-01 ~ $ ./etcdcl
//respGet, err: <nil>,100: Key not found (/test) [283]respCreate,err: &{Action:create Node:0xc208070840 PrevNode:<nil> EtcdIndex:284 RaftIndex:1268 RaftTerm:0},%!s(<nil>)respCreate,err: <nil>,105: Key already exists (/test) [284]respGet,err%!(EXTRA *etcd.Response=&{get 0xc208070c60 <nil> 284 1269 0}, <nil>)

// etcd host and me should both be in the "X.X.X.X:P" format
func EtcdNode(etcdHost string, me string, dataDir string, mountPoint string) (Service, error) {
	client := etcd.NewClient(machines)
	defer client.Close()
	masterNodeLoc := "/mfs/master/current"
	// check master node
	masterGetResp, err := client.get(masterNodeLoc, false, false)
	if err != nil {
		return nil, err
	}
	masterNode := masterGetResp.Node

	// the value of masterNode is one of:  nil, "X.X.X.X:P:ELECTED", "X.X.X.X:P:UP"
	// nil means nobody elected, host:port:ELECTED when a leader is coming online
	// and host:port:UP when safe to connect to leader.
	if masterNode == nil {
		// call leader election and serve either master or peer
		return leadOrJoin(client, me, dataDir, mountPoint)
	} else {
		return servePeer(masterNode, client, me, dataDir, mountPoint)
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
		ret,err := NewMaster(cfg)
		if err != nil {
			etcd.Set("/mfs/masterAddr", me + ":UP", 1e12)
		} else {
			etcd.Set("/mfs/masterAddr", me + fmt.Sprintf(":ERROR:%s,err)")
		}
		return ret,err
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
// first arg is last known val for master
// waits till master definitely up until return
func servePeer(masterNode etcd.Node, client *etcd.Client, me string, dataDir string, mountPoint string) (Service,error) {
	// loop until master is up
	for {
		if strings.Count(masterNode.Value, ":") != 2 {
			return nil, fmt.Errorf("Should have either INIT or a host:port:status tuple in etcd node %s -- had %s", masterNodeLoc, masterNode.Value)
		}
		hostPortStatus = strings.Split(masterNode.Value, ":")
		host := hostPortStatus[0]
		port := hostPortStatus[1]
		stat := hostPortStatus[2]
		if stat == "ELECTED" {
			// keep checking until we're UP
			time.Sleep(1 * time.Second)
			masterNode,err = client.Get("/mfs/masterNode",false,false)
			continue
		} else {
			if stat == "UP" {
				goto MASTERUP
			}
		}

	}
	MASTERUP: 
	hostPortStatus = strings.Split(masterNode.Value, ":")
	host := hostPortStatus[0]
	port := hostPortStatus[1]
	stat := hostPortStatus[2]
	if stat != "UP" {
		return nil,fmt.Errorf("Master not actually up when trying to connect!  masterNodeStat: " + masterNode.Value)
	}
	masterAddr := host + ":" + port
	ret,err := NewPeer(
		&PeerConfig {
			MasterAddr: masterAddr,
			BindAddr: me,
			VolumeRoots: []string{dataDir},
			MountPoint: mountPoint,
		}, false)
	return ret,err
}

func NewEmbedEtcd(listenAddr string) (Service,error) {
	
}

type EmbedEtcd struct {

}

func (e *EmbedEtcd) Serve() error {

}

func (e *EmbedEtcd) Close() error {

}

func (e *EmbedEtcd) WaitClosed() error {

}