package test

import (
	"crypto/rand"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"os"
	"sync"
	"syscall"
	"testing"
)

type testReadPipe struct {
	responseCode int32
	b            []byte
	numWritten   int
	done         chan bool
	l            *sync.Mutex
}

func newTestReadPipe() *testReadPipe {
	return &testReadPipe{0, make([]byte, 128*1024, 128*1024), 0, make(chan bool), new(sync.Mutex)}
}

func (t *testReadPipe) Reset() {
	t.l.Lock()
	defer t.l.Unlock()
	t.responseCode = 0
	t.numWritten = 0
	t.done = make(chan bool, 1)
}

func (t *testReadPipe) WriteHeader(code int32, returnBytesLength int) error {
	t.l.Lock()
	defer t.l.Unlock()
	fmt.Printf("Writing header to mock, code %d returnBytesLength %d\n", code, returnBytesLength)
	t.responseCode = code
	if len(t.b) < returnBytesLength {
		t.b = make([]byte, returnBytesLength, returnBytesLength)
	}
	t.numWritten = 0
	fmt.Printf("Done writing header to mock, numWritten %d\n", t.numWritten)
	return nil
}

func (t *testReadPipe) Write(b []byte) (int, error) {
	t.l.Lock()
	defer t.l.Unlock()
	copy(b, t.b[t.numWritten:])
	t.numWritten += len(b)
	return len(b), nil
}

func (t *testReadPipe) LoadFrom(fd uintptr, length int) (int, error) {
	t.l.Lock()
	defer t.l.Unlock()
	fmt.Printf("Splicing from %d to %d into array of length %d\n", t.numWritten, t.numWritten+length, len(t.b))
	ret1, ret2 := syscall.Read(int(fd), t.b[t.numWritten:t.numWritten+length])
	//if len(t.b) > 5 {
	//	fmt.Printf("First 5 in test splice buffer : %x\n", t.b[:5])
	//}
	t.numWritten += ret1
	fmt.Printf("Spliced %d, returning\n", ret1)
	return ret1, ret2
}

func (t *testReadPipe) LoadFromAt(fd uintptr, length int, offset int64) (int, error) {
	t.l.Lock()
	defer t.l.Unlock()

	ret1, ret2 := syscall.Pread(int(fd), t.b[t.numWritten:t.numWritten+length], offset)
	t.numWritten += ret1
	return ret1, ret2
}

func (t *testReadPipe) Commit() error {
	t.l.Lock()
	done := t.done
	t.done = nil
	t.l.Unlock()
	done <- true
	close(done)
	return nil
}

func (t *testReadPipe) waitDone() {
	t.l.Lock()
	done := t.done
	t.l.Unlock()
	if done != nil {
		_, _ = <-done
	}
}

//func TestWriteRead(t *testing.T) {
//	fmt.Println("testWriteRead")
//	fmt.Println("Adding node to cluster")
//	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
//	id, err := testCluster.Names.AddInode(ino)
//	if err != nil {
//		t.Fatal(err)
//	}
//	ino.Inodeid = id

//	writefd, err := openFiles.Open(ino.Inodeid, true)
//	fmt.Printf("Opened file with fd %d\n", writefd)
//	if err != nil {
//		t.Fatal(err)
//	}
//	// 200 MB to make us 2 blocks
//	bytes := make([]byte, 1024*1024*200)
//	fmt.Printf("Getting 200MB from rand\n")
//	_, err = rand.Read(bytes)
//	if err != nil {
//		t.Fatal(err)
//	}
//	fmt.Printf("first 5 before write %x\n", bytes[:5])
//	fmt.Println("Writing some bytes")
//	n, err := openFiles.Write(writefd, bytes, 0, uint32(len(bytes)))
//	if err != nil {
//		t.Fatal(err)
//	}
//	if n < uint32(len(bytes)) {
//		t.Fatal(fmt.Sprintf("Only wrote %d bytes out of %d", n, len(bytes)))
//	}

//	err = openFiles.Sync(writefd)
//	if err != nil {
//		t.Fatal(err)
//	}

//	readBytes := &testReadPipe{0, nil, 0, new(sync.Mutex)}
//	err = openFiles.Read(writefd, readBytes, 0, 1024*1024*200)
//	if err != nil {
//		t.Fatal(err)
//	}
//	for idx, b := range readBytes.b {
//		if b != bytes[idx] {
//			t.Fatal(fmt.Sprintf("Bytes not equal at offset %d : %x != %x", idx, b, bytes[idx]))
//		}
//	}
//}

func TestWriteRead2(t *testing.T) {
	fmt.Println("testWriteRead2")
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id
	writefd, err := openFiles.Open(ino.Inodeid, true)
	// we wnat to write a bunch of bytes the way the fs does it, in chunks of 65536

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
	for i := 0; i < 5000; i++ {
		// reset our buffer
		readBytes.Reset()
		// fill it
		err := openFiles.Read(writefd, readBytes, uint64(65536*i), 65536)
		if err != nil {
			t.Fatal(err)
		}
		readBytes.waitDone()
		for idx := 0; idx < 65536; idx++ {
			if readBytes.b[idx] != bytes[idx] {
				fmt.Printf("Bytes at beginning:  %x : %x\n", readBytes.b[:5], bytes[:5])
				//fmt.Printf("Bytes near offest:  %x : %x\n",readBytes[idx-5:idx+5],bytes[idx-5:idx+5])
				t.Fatal(fmt.Sprintf("Bytes not equal at offset %d, iteration %d : %x != %x", idx, i, readBytes.b[idx], bytes[idx]))
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

	writefd, err := openFiles.Open(ino.Inodeid, true)
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
	n, err := openFiles.Write(writefd, bytes, 0, uint32(len(bytes)))
	if err != nil {
		t.Fatal(err)
	}
	if n < uint32(len(bytes)) {
		t.Fatal(fmt.Sprintf("Only wrote %d bytes out of %d", n, len(bytes)))
	}

	err = openFiles.Sync(writefd)
	if err != nil {
		t.Fatal(err)
	}

	readBytes := newTestReadPipe()
	fmt.Printf("Reading %d bytes\n", n)
	err = openFiles.Read(writefd, readBytes, 0, 5)
	if err != nil {
		t.Fatal(err)
	}

	readBytes.waitDone()
	for idx := 0; idx < 5; idx++ {
		if readBytes.b[idx] != bytes[idx] {
			t.Fatal(fmt.Sprintf("Bytes not equal at offset %d : %x != %x", idx, readBytes.b[idx], bytes[idx]))
		}
	}
	fmt.Println("Done TestShortRead")
}
