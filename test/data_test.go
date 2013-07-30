package test

import (
	"crypto/rand"
	"fmt"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/maggiefs"
	"os"
	"testing"
)

func TestWriteRead(t *testing.T) {
	fmt.Println("testWriteRead")
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id

	writer, err := client.NewInodeWriter(ino.Inodeid, testCluster.Leases, testCluster.Names, testCluster.Datas)
	if err != nil {
		t.Fatal(err)
	}
	// 200 MB to make us 2 blocks
	bytes := make([]byte, 1024*1024*200)
	_, err = rand.Read(bytes)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("first 5 before write %x\n", bytes[:5])
	fmt.Println("Writing some bytes")
	n, err := writer.WriteAt(bytes, 0, uint32(len(bytes)))
	if err != nil {
		t.Fatal(err)
	}
	if n < uint32(len(bytes)) {
		t.Fatal(fmt.Sprintf("Only wrote %d bytes out of %d", n, len(bytes)))
	}

	readBytes := make([]byte, 1024*1024*200)
	r, err := client.NewReader(ino.Inodeid, testCluster.Names, testCluster.Datas)
	if err != nil {
		t.Fatal(fmt.Sprintf("Error opening reader %s", err.Error()))
	}
	_, err = r.ReadAt(readBytes, 0, 0, 1024*1024*200)
	if err != nil {
		t.Fatal(err)
	}
	for idx, b := range readBytes {
		if b != bytes[idx] {
			t.Fatal(fmt.Sprintf("Bytes not equal at offset %d : %x != %x", idx, b, bytes[idx]))
		}
	}
}

func TestWriteRead2(t *testing.T) {
	fmt.Println("testWriteRead2")
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id
	writer, err := client.NewInodeWriter(ino.Inodeid, testCluster.Leases, testCluster.Names, testCluster.Datas)
	if err != nil {
		t.Fatal(err)
	}
	// we wnat to write a bunch of bytes the way the fs does it, in chunks of 65536

	bytes := make([]byte, 65536)
	n, err := rand.Read(bytes)
	if n < len(bytes) {
	 t.Fatal(fmt.Errorf("Only returned %d bytes in call to rand.Read",n))
	}
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		n, err := writer.WriteAt(bytes, uint64(65536*i), uint32(len(bytes)))
		if err != nil {
			t.Fatal(err)
		}
		if n < uint32(len(bytes)) {
			t.Fatal(fmt.Sprintf("Only wrote %d bytes out of %d", n, len(bytes)))
		}
	}

	// then do that many reads across the file to confirm it's ok
	readBytes := make([]byte, 65536)
	reader,err := client.NewReader(ino.Inodeid, testCluster.Names, testCluster.Datas)
	if err != nil {
		t.Fatal(fmt.Sprintf("Error opening reader %s", err.Error()))
	}
	for i := 0; i < 5000; i++ {
		n, err := reader.ReadAt(readBytes, uint64(65536*i), 0, uint32(len(readBytes)))
		if err != nil {
			t.Fatal(err)
		}
		if n < uint32(len(readBytes)) {
			t.Fatal(fmt.Sprintf("Only read %d bytes out of %d", n, len(readBytes)))
		}
		for idx := 0; idx < 65536; idx++ {
			if readBytes[idx] != bytes[idx] {
			  fmt.Printf("Bytes at beginning:  %x : %x\n",readBytes[:5],bytes[:5])
			  //fmt.Printf("Bytes near offest:  %x : %x\n",readBytes[idx-5:idx+5],bytes[idx-5:idx+5])
				t.Fatal(fmt.Sprintf("Bytes not equal at offset %d, iteration %d : %x != %x", idx, i, readBytes[idx], bytes[idx]))
			}
		}
	}
	fmt.Println("Done TestReadWrite")
}

func TestShortRead(t *testing.T) {
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id

	writer, err := client.NewInodeWriter(ino.Inodeid, testCluster.Leases, testCluster.Names, testCluster.Datas)
	if err != nil {
		t.Fatal(err)
	}
	// 5 bytes
	bytes := make([]byte, 5)
	_, err = rand.Read(bytes)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Writing some bytes")
	n, err := writer.WriteAt(bytes, 0, uint32(len(bytes)))
	if err != nil {
		t.Fatal(err)
	}
	if n < uint32(len(bytes)) {
		t.Fatal(fmt.Sprintf("Only wrote %d bytes out of %d", n, len(bytes)))
	}

	readBytes := make([]byte, 4096)
	r, err := client.NewReader(ino.Inodeid, testCluster.Names, testCluster.Datas)
	if err != nil {
		t.Fatal(fmt.Sprintf("Error opening reader %s", err.Error()))
	}
	n, err = r.ReadAt(readBytes, 0, 0, 5)
	fmt.Printf("Read %d bytes\n", n)
	if err != nil {
		t.Fatal(err)
	}

	for idx := 0; idx < 5; idx++ {
		if readBytes[idx] != bytes[idx] {
			t.Fatal(fmt.Sprintf("Bytes not equal at offset %d : %x != %x", idx, readBytes[idx], bytes[idx]))
		}
	}
	fmt.Println("Done TestShortRead")
}
