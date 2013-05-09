package integration

// test inode methods

// replication methods are in replication_test.go

import (
  "github.com/jbooth/maggiefs/maggiefs"
	"testing"
	"fmt"
)

var (
	cfg NNConfig = NNConfig{
		NameBindAddr:"0.0.0.0:11001",
		LeaseBindAddr:"0.0.0.0:11002",
		NNHomeDir:"/tmp/namehome",
		ReplicationFactor:3,
	}
	ns *NameLeaseServer = nil
	client maggiefs.NameService = nil
)

func initNS() {
	var err error
	ns,err = NewNameServer(&cfg,true)
	if err != nil {
		panic(err)
	}
	ns.Start()
	fmt.Printf("getting name client for addr %s\n",cfg.NameBindAddr)
	client,err = NewNameClient(cfg.NameBindAddr)
	
	if err != nil {
		panic(err)
	}	
}

func teardownNS() {
	err := ns.Close()
	if err != nil { panic(err) }
	err = ns.WaitClosed()
	if err != nil { panic(err) }
	// todo should have close methods on client somehow
}

func TestAddInode(t *testing.T) {
	fmt.Println("setting up")
	initNS()
	fmt.Println("tearing down")
	teardownNS()
}




