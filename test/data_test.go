package test

import (
  "github.com/jbooth/maggiefs/maggiefs"
  "github.com/jbooth/maggiefs/client"
	"testing"
	"fmt"
	"os"
	"crypto/rand"
)


func TestWriteRead (t *testing.T) {
	initCluster()
	defer teardownCluster()
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id
	
	writer,err := client.NewInodeWriter(ino.Inodeid,testCluster.Leases,testCluster.Names,testCluster.Datas)
	if err != nil {
		t.Fatal(err)
	}
	// 200 MB to make us 2 blocks
	bytes := make([]byte,1024*1024*200)
	_,err = rand.Read(bytes)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("first 5 before write %x\n",bytes[:5])
	fmt.Println("Writing some bytes")
	n,err := writer.WriteAt(bytes,0,uint32(len(bytes)))
	if err != nil {
		t.Fatal(err)
	}
	if n < uint32(len(bytes)) {
		t.Fatal(fmt.Sprintf("Only wrote %d bytes out of %d",n,len(bytes)))
	}
	
	readBytes := make([]byte,1024*1024*200)
	r,err := client.NewReader(ino.Inodeid,testCluster.Names,testCluster.Datas)
	if err != nil {
		t.Fatal(fmt.Sprintf("Error opening reader %s",err.Error()))
	}
	_,err = r.ReadAt(readBytes,0,0,1024*1024*200)
	if err != nil {
		t.Fatal(err)
	}
	for idx,b := range readBytes {
		if b != bytes[idx] {
			t.Fatal(fmt.Sprintf("Bytes not equal at offset %d : %x != %x",idx,b,bytes[idx]))
		}
	}
}

func TestShortRead (t *testing.T) {
	initCluster()
	defer teardownCluster()
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id
	
	writer,err := client.NewInodeWriter(ino.Inodeid,testCluster.Leases,testCluster.Names,testCluster.Datas)
	if err != nil {
		t.Fatal(err)
	}
	// 200 MB to make us 2 blocks
	bytes := make([]byte,5)
	_,err = rand.Read(bytes)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Writing some bytes")
	n,err := writer.WriteAt(bytes,0,uint32(len(bytes)))
	if err != nil {
		t.Fatal(err)
	}
	if n < uint32(len(bytes)) {
		t.Fatal(fmt.Sprintf("Only wrote %d bytes out of %d",n,len(bytes)))
	}
	
	readBytes := make([]byte,4096)
	r,err := client.NewReader(ino.Inodeid,testCluster.Names,testCluster.Datas)
	if err != nil {
		t.Fatal(fmt.Sprintf("Error opening reader %s",err.Error()))
	}
	n,err = r.ReadAt(readBytes,0,0,4096)
	
	if err != nil {
		t.Fatal(err)
	}
	
	for idx := 0 ; idx < 5 ; idx++ {
		if readBytes[idx] != bytes[idx] {
			t.Fatal(fmt.Sprintf("Bytes not equal at offset %d : %x != %x",idx,readBytes[idx],bytes[idx]))
		}
	}
}