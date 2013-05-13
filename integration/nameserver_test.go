package integration

// test inode methods

// replication methods are in replication_test.go

import (
  "github.com/jbooth/maggiefs/maggiefs"
	"testing"
	"fmt"
	"os"
	"time"
)

var (
	cfg NNConfig = NNConfig{
		NameBindAddr:"0.0.0.0:11001",
		LeaseBindAddr:"0.0.0.0:11002",
		NNHomeDir:"/tmp/namehome",
		ReplicationFactor:3,
	}
	ns *NameLeaseServer = nil
	ncli maggiefs.NameService = nil
)

func initNS() {
	var err error
	ns = nil
	ns,err = NewNameServer(&cfg,true)
	if err != nil {
		panic(err)
	}
	fmt.Printf("created nameserver %+v\n",ns)
	ns.Start()
	fmt.Printf("getting name client for addr %s\n",cfg.NameBindAddr)
	ncli,err = NewNameClient(cfg.NameBindAddr)
	
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
	id,err := ncli.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	
	ino2,err := ncli.GetInode(id)
	if ! ino.Equals(ino2) {
		t.Fatal(fmt.Errorf("Error, inodes not equal : %+v : %+v\n",*ino, *ino2))
	}
}

func TestSetInode(t *testing.T) {
	fmt.Println("setting up")
	initNS()
	defer teardownNS()
	ino := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err := ncli.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	ino.Mtime = time.Now().Unix()
	err = ncli.SetInode(ino)
	if err != nil {
		panic(err)
	}
	ino2,err := ncli.GetInode(id)
	if ! ino.Equals(ino2) {
		t.Fatal(fmt.Errorf("Error, inodes not equal : %+v : %+v\n",*ino, *ino2))
	}
}

func TestLink(t *testing.T) {
	
	fmt.Println("testing link")
	initNS()
	defer teardownNS()
	ino := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err := ncli.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	
	ino2 := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err = ncli.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino2.Inodeid = id
	
	// test normal link
	fmt.Println("linking")
	err = ncli.Link(ino.Inodeid,ino2.Inodeid,"name",true)
	if err != nil {
		panic(err)
	}
	ino,err = ncli.GetInode(ino.Inodeid)
	if err != nil {
	  panic(err) 
	}
	if ino.Children["name"].Inodeid != ino2.Inodeid {
		t.Fatalf("Didn't link properly!")
	}
	// test an unforced attempt to overwrite
	
	// test overwriting forced
	
}

func TestUnlink(t *testing.T) {
		fmt.Println("testing link")
	initNS()
	defer teardownNS()
	ino := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err := ncli.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	
	ino2 := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err = ncli.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino2.Inodeid = id
	
	// test normal link
	fmt.Println("linking")
	err = ncli.Link(ino.Inodeid,ino2.Inodeid,"name",true)
	if err != nil {
		panic(err)
	}
	ino,err = ncli.GetInode(ino.Inodeid)
	if err != nil {
	  panic(err) 
	}
	if ino.Children["name"].Inodeid != ino2.Inodeid {
		t.Fatalf("Didn't link properly!")
	}
	
	err = ncli.Unlink(ino.Inodeid,"name")
	if err != nil {
		panic(err)
	}
	ino,err = ncli.GetInode(ino.Inodeid)
	if err != nil {
	  panic(err) 
	}
	_,ok := ino.Children["name"]
  if ok {
		t.Fatalf("Didn't unlink properly!")
	}
}




