package main

import ()

import (
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/integration"
	"os"
	"fmt"
)

var (
	testCluster  *integration.SingleNodeCluster
	mount        *integration.Mount
	svcHandle    *integration.MultiService
	mountPoint   string     = "/tmp/mfsTestMount"
	serviceError chan error = make(chan error, 1)
)

func main() {
	setup()
	defer func() {
		if x := recover(); x != nil {
			fmt.Printf("run time panic running test: %v\nshutting down\n", x)
			teardown()
			panic(x)
		} else {
			teardown()
		}
	}()
	shortFileTest()
}
func setup() {
	// set up cluster
	os.RemoveAll("/tmp/testcluster")
	var err error
	testCluster, err = integration.NewSingleNodeCluster(4, 2, 3, "/tmp/testcluster")
	if err != nil {
		panic(err)
	}
	// set up mountpoint
	maggieFuse, err := client.NewMaggieFuse(testCluster.Leases, testCluster.Names, testCluster.Datas)
	if err != nil {
		return
	}
	os.RemoveAll(mountPoint)
	os.MkdirAll(mountPoint,0777)
	mount, err = integration.NewMount(maggieFuse, mountPoint, true)
	if err != nil {
		return
	}
	svcHandle = integration.NewMultiService()
	svcHandle.AddService(testCluster)
	svcHandle.AddService(mount)
	// spin off service function
	go func() {
		serviceError <- svcHandle.Serve()
	}()
}

func teardown() {
	svcHandle.Close()
	svcHandle.WaitClosed()
	os.RemoveAll("/tmp/testcluster")
	os.RemoveAll(mountPoint)
}

func shortFileTest() error {
	f,err := os.Create(mountPoint + "/shortFile.txt")
	if err != nil {
		return err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file for shortRead : %s\n",err.Error())
		}
		fmt.Printf("Closed file %s\n",f.Name())
	}()
	_,err = f.WriteString("hi")
	if err != nil {
		return err
	}
	// assert length
	fstat,err := f.Stat()
	if err != nil {
		return err
	}
	if fstat.Size() != 2 {
		return fmt.Errorf("Wrong file size!")
	}
	_,err = f.Seek(0,0)
	if err != nil {
		return err
	}
	bytes := make([]byte,2)
	f.Read(bytes)
	if string(bytes) != "hi" {
		return fmt.Errorf("Wrong file contents!  Expected: hi, got %s",string(bytes))
	}
	fmt.Println("success shortFileTest")
	return nil
}

func largeFileTest() error {
	return nil
}