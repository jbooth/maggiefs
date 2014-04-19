package test

import (
	"crypto/rand"
	"fmt"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/integration"
	"github.com/jbooth/maggiefs/maggiefs"
	"os"
	"testing"
)

var (
	testClusterWithBadNode *integration.SingleNodeCluster
	openFilesWithBadNode   *client.OpenFileMap
)

func initBadNodeCluster() {
	os.RemoveAll("/tmp/testclusterWithBadNode")
	var err error
	//testCluster, err = integration.NewSingleNodeCluster(4, 2, 3, "/tmp/testcluster", "", true)
	testClusterWithBadNode, err = integration.NewSingleNodeCluster(12004, 4, 2, 3, "/tmp/testclusterWithBadNode", "", true)
	if err != nil {
		panic(err)
	}
	openFilesWithBadNode = client.NewOpenFileMap(testCluster.Leases, testCluster.Names, testCluster.Datas, nil, func(i uint64) {})

	go func() {
		fmt.Printf("Starting test cluster\n")
		testClusterWithBadNode.Serve()
	}()
}

func TestWriteReadWithNodeFailure(t *testing.T) {
	initBadNodeCluster()
	openFiles := openFilesWithBadNode
	testCluster := testClusterWithBadNode

	fmt.Println("testWriteRead2")
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id
	// add a block that's guaranteed to be on the first dn
	firstDnStat, err := testCluster.DataNodes[0].HeartBeat()
	if err != nil {
		t.Fatal(err)
	}
	firstDnId := firstDnStat.DnId
	// add a block that's forced to be on that DN
	ino, err = testCluster.Names.AddBlock(ino.Inodeid, 0, &firstDnId)

	// now start writing
	// we wnat to write a bunch of bytes the way the fs does it, in chunks of 65536
	writefd, err := openFiles.Open(ino.Inodeid, true)
	bytes := make([]byte, 65536)
	n, err := rand.Read(bytes)
	if n < len(bytes) {
		t.Fatal(fmt.Errorf("Only returned %d bytes in call to rand.Read", n))
	}
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5000; i++ {
		n, err := openFiles.Write(writefd, bytes, uint64(65536*i), uint32(len(bytes)))
		if err != nil {
			t.Fatal(err)
		}
		if n < uint32(len(bytes)) {
			t.Fatal(fmt.Sprintf("Only wrote %d bytes out of %d", n, len(bytes)))
		}
	}
	err = openFiles.Sync(writefd)
	if err != nil {
		t.Fatal(err)
	}
	// then do that many reads across the file to confirm it's ok
	readBytes := newTestReadPipe()
	errChan := make(chan error)
	// spin off goroutine to read, while we kill stuff
	go func() {
		for i := 0; i < 5000; i++ {
			// reset our buffer
			readBytes.Reset()
			// fill it
			err := openFiles.Read(writefd, readBytes, uint64(65536*i), 65536)
			if err != nil {
				errChan <- err
				return
			}
			readBytes.waitDone()
			for idx := 0; idx < 65536; idx++ {
				if readBytes.b[idx] != bytes[idx] {
					fmt.Printf("Bytes at beginning:  %x : %x\n", readBytes.b[:5], bytes[:5])
					//fmt.Printf("Bytes near offest:  %x : %x\n",readBytes[idx-5:idx+5],bytes[idx-5:idx+5])
					errChan <- fmt.Errorf("Bytes not equal at offset %d, iteration %d : %x != %x", idx, i, readBytes.b[idx], bytes[idx])
				}
			}
		}
		errChan <- nil
	}()
	// blow away the first DN
	fmt.Println("Killing a datanode")
	testCluster.DataNodeServs[0].Close()
	// wait for our reads to finish
	fmt.Println("Waiting for reads to finish")
	err = <-errChan
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("Reads done")
	fmt.Println("Done TestReadWithNodeFail")
}
