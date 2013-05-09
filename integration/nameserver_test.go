package integration

// test inode methods

// replication methods are in replication_test.go

import (
  "github.com/jbooth/maggiefs/maggiefs"
	"testing"
	"fmt"
	"reflect"
	"os"
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
	fmt.Printf("created nameserver %+v\n",ns)
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
	defer teardownNS()
	ino := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err := client.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	
	ino2,err := client.GetInode(id)
	if ! reflect.DeepEqual(ino,ino2) {
		t.Fatal(fmt.Errorf("Error, inodes not equal : %+v : %+v\n",ino, ino2))
	}
}




