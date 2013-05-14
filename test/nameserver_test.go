package test

// test inode methods

// replication methods are in replication_test.go

import (
  "github.com/jbooth/maggiefs/maggiefs"
	"testing"
	"fmt"
	"os"
	"time"
)
func TestAddInode(t *testing.T) {
	fmt.Println("setting up")
	initCluster()
	defer teardownCluster()
	ino := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err := testCluster.Names.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	
	ino2,err := testCluster.Names.GetInode(id)
	if ! ino.Equals(ino2) {
		t.Fatal(fmt.Errorf("Error, inodes not equal : %+v : %+v\n",*ino, *ino2))
	}
}

func TestSetInode(t *testing.T) {
	fmt.Println("setting up")
	initCluster()
	defer teardownCluster()
	ino := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err := testCluster.Names.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	ino.Mtime = time.Now().Unix()
	err = testCluster.Names.SetInode(ino)
	if err != nil {
		panic(err)
	}
	ino2,err := testCluster.Names.GetInode(id)
	if ! ino.Equals(ino2) {
		t.Fatal(fmt.Errorf("Error, inodes not equal : %+v : %+v\n",*ino, *ino2))
	}
}

func TestLink(t *testing.T) {
	
	fmt.Println("testing link")
	initCluster()
	defer teardownCluster()
	ino := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err := testCluster.Names.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	
	ino2 := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err = testCluster.Names.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino2.Inodeid = id
	
	// test normal link
	fmt.Println("linking")
	err = testCluster.Names.Link(ino.Inodeid,ino2.Inodeid,"name",true)
	if err != nil {
		panic(err)
	}
	ino,err = testCluster.Names.GetInode(ino.Inodeid)
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
	initCluster()
	defer teardownCluster()
	ino := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err := testCluster.Names.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	
	ino2 := maggiefs.NewInode(0,maggiefs.FTYPE_REG,0755,uint32(os.Getuid()),uint32(os.Getgid()))
	id,err = testCluster.Names.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino2.Inodeid = id
	
	// test normal link
	fmt.Println("linking")
	err = testCluster.Names.Link(ino.Inodeid,ino2.Inodeid,"name",true)
	if err != nil {
		panic(err)
	}
	ino,err = testCluster.Names.GetInode(ino.Inodeid)
	if err != nil {
	  panic(err) 
	}
	if ino.Children["name"].Inodeid != ino2.Inodeid {
		t.Fatalf("Didn't link properly!")
	}
	
	err = testCluster.Names.Unlink(ino.Inodeid,"name")
	if err != nil {
		panic(err)
	}
	ino,err = testCluster.Names.GetInode(ino.Inodeid)
	if err != nil {
	  panic(err) 
	}
	_,ok := ino.Children["name"]
  if ok {
		t.Fatalf("Didn't unlink properly!")
	}
}




