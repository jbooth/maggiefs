package main

import ()

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/integration"
	"os"
	"os/signal"
	"syscall"
)

var (
	testCluster  *integration.SingleNodeCluster
	mount        *integration.Mount
	svcHandle    *integration.MultiService
	mountPoint   string     = "/tmp/mfsTestMount2"
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
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGPIPE, syscall.SIGHUP)
	go func() {
		s := <-sig
		fmt.Printf("received signal: %d\n", s)
		teardown()
	}()
	err := shortFileTest()
	if err != nil {
		fmt.Println("FAIL shortFileTest")
		fmt.Println(err)
		os.Exit(1)
	}
	err = longFileTest()
	if err != nil {
		fmt.Println("FAIL longFileTest")
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("Passed!")
}
func setup() {
	// set up cluster
	os.RemoveAll("/tmp/testcluster")
	var err error
	testCluster, err = integration.NewSingleNodeCluster(4, 2, 3, "/tmp/testcluster")
	if err != nil {
		fmt.Println("failed to set up")
		fmt.Println(err)
		os.Exit(1)
	}
	// set up mountpoint
	maggieFuse, err := client.NewMaggieFuse(testCluster.Leases, testCluster.Names, testCluster.Datas, nil)
	if err != nil {
		return
	}
	os.RemoveAll(mountPoint)
	os.MkdirAll(mountPoint, 0777)
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
	f, err := os.Create(mountPoint + "/shortFile.txt")
	if err != nil {
		return err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file for shortRead : %s\n", err.Error())
		}
		fmt.Printf("Closed file %s\n", f.Name())
	}()
	_, err = f.WriteString("hi")
	if err != nil {
		return err
	}
	// assert length
	fstat, err := f.Stat()
	if err != nil {
		return err
	}
	if fstat.Size() != 2 {
		return fmt.Errorf("Wrong file size! Expected 2, got %d", fstat.Size())
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		return err
	}
	bytes := make([]byte, 2)
	f.Read(bytes)
	if string(bytes) != "hi" {
		return fmt.Errorf("Wrong file contents!  Expected: hi, got %s", string(bytes))
	}
	fmt.Println("success shortFileTest")
	return nil
}

func longFileTest() error {
	f, err := os.Create(mountPoint + "/longFile.txt")
	if err != nil {
		return err
	}
	defer func() {
		err := f.Close()
		if err != nil {
			fmt.Printf("Error closing file for shortRead : %s\n", err.Error())
		}
		fmt.Printf("Closed file %s\n", f.Name())
	}()
	randBytes := make([]byte, 1024*63) // 63kb to stress edge cases
	_, err = rand.Read(randBytes)
	fSize := int64(0)
	for i := 0; i < 4096; i++ { //63kb * 4096 ~= 256MB
		_, err = f.Write(randBytes)
		if err != nil {
			return err
		}
		fSize += int64(len(randBytes))
	}
	// add another 1337 for odd number to test block boundaries
	_, err = f.Write(randBytes[:1337])
	if err != nil {
		return err
	}
	fSize += 1337
	// assert length
	fstat, err := f.Stat()
	if err != nil {
		return err
	}
	if fstat.Size() != fSize {
		return fmt.Errorf("Wrong file size!")
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		return err
	}
	readBytes := make([]byte, 1024*63)
	for i := 0; i < 4096; i++ {
		_, err = f.Read(readBytes)
		if err != nil {
			return err
		}
		if !bytes.Equal(randBytes, readBytes) {
			return fmt.Errorf("bytes not equal to reference at index %d", i)
		}
	}
	// last 1337
	_, err = f.Read(readBytes[:1337])
	if err != nil {
		return err
	}
	if !bytes.Equal(randBytes[:1337], readBytes[:1337]) {
		return fmt.Errorf("bytes not equal to reference in last section")
	}
	fmt.Println("success longFileTest")
	return nil
}
