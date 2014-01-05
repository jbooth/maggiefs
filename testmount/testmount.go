package main

import ()

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"github.com/jbooth/maggiefs/integration"
	"os"
	"os/signal"
	"syscall"
)

var (
	testCluster  *integration.SingleNodeCluster
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
		return
	}
	err = longFileTest()
	if err != nil {
		fmt.Println("FAIL longFileTest")
		fmt.Println(err)
		return
	}
	fmt.Println("Passed!")
}
func setup() {
	// set up cluster
	os.RemoveAll("/tmp/testcluster")
	var err error
	os.RemoveAll(mountPoint)
	os.MkdirAll(mountPoint, 0777)
	testCluster, err = integration.NewSingleNodeCluster(4, 2, 3, "/tmp/testcluster", "/tmp/mfsTestMount",true)
	if err != nil {
		fmt.Println("failed to set up")
		fmt.Println(err)
		os.Exit(1)
	}
	go func() {
		serviceError <- testCluster.Serve()
	}()
}

func teardown() {
	testCluster.Close()
	testCluster.WaitClosed()
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
  err = f.Sync()
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
	fmt.Printf("Just read bytes %x\n",bytes)
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
		
		fmt.Printf("Wrote %d bytes, syncing..\n",len(randBytes))
  		f.Sync()
		fSize += int64(len(randBytes))
		// check size at each step	
		fstat, err := f.Stat()
		if err != nil {
			return err
		}
		fmt.Printf("Finished write, expected size: %d, actual size %d\n",fSize,fstat.Size())
		if fstat.Size() != fSize {
			return fmt.Errorf("Wrong file size! expected %d got %d", fSize, fstat.Size())
		}
	}
	// add another 1337 for odd number to test block boundaries
	_, err = f.Write(randBytes[:1337])
	if err != nil {
		return err
	}
	fSize += 1337
	
  	f.Sync()
	
	// assert length after last write
	fstat, err := f.Stat()
	if err != nil {
		return err
	}
	if fstat.Size() != fSize {
		return fmt.Errorf("Wrong file size! expected %d got %d", fSize, fstat.Size())
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
	_, _ = f.Seek(0, 0)
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
