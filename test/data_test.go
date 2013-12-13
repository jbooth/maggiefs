package test

import (
	"crypto/rand"
	"fmt"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/maggiefs"
	"os"
	"testing"
)

type testReadPipe struct {
	responseCode int32
	b []byte
	numWritten int
}

func (t *testReadPipe) WriteHeader(code int32, returnBytesLength int) error {
	t.responseCode = code
	t.b = make([]byte,returnBytesLength,returnBytesLength)
	t.numWritten = 0
	return nil
}

func (t *testReadPipe) WriteBytes(b []byte) (int,error) {
	copy(b,t.b[t.numWritten:])
	t.numWritten += len(b)
	return len(b),nil
}

func (t *testReadPipe) SpliceBytes(fd uintptr, length int) (int,error) {
	f := os.NewFile(fd,"splicingFrom")
	ret1,ret2 := f.Read(t.b[t.numWritten:t.numWritten+length])
	t.numWritten += ret1
	return ret1,ret2
}

func (t *testReadPipe) SpliceBytesAt(fd uintptr, length int, offset int64) (int,error) {
	f := os.NewFile(fd,"splicingFrom")
	ret1,ret2 := f.ReadAt(t.b[t.numWritten:t.numWritten+length],offset)
	t.numWritten += ret1
	return ret1,ret2
}

// represents an OS pipe which read results will be piped through
type ReadPipe interface {
	// write the header for this response
	WriteHeader(code int32, returnBytesLength int) error
	// write bytes from memory
	WriteBytes(b []byte) (int,error)
	// splice bytes from the given fd
	SpliceBytes(fd uintptr, length int) (int,error)
	// splice bytes from the given fd at the given offset
	SpliceBytesAt(fd uintptr, length int, offset int64) (int,error)
}


func TestWriteRead(t *testing.T) {
	fmt.Println("testWriteRead")
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id

	writer, err := client.NewInodeWriter(ino.Inodeid, testCluster.Leases, testCluster.Names, testCluster.Datas,nil)
	if err != nil {
		t.Fatal(err)
	}
	// 200 MB to make us 2 blocks
	bytes := make([]byte, 1024*1024*200)
	fmt.Printf("Getting 200MB from rand\n")
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
	
	err = writer.Fsync()
	if err != nil { t.Fatal(err) }
	
	readBytes := &testReadPipe{}
	r, err := client.NewReader(ino.Inodeid, testCluster.Names, testCluster.Datas)
	if err != nil {
		t.Fatal(fmt.Sprintf("Error opening reader %s", err.Error()))
	}
	err = r.ReadAt(readBytes, 0, 1024*1024*200)
	if err != nil {
		t.Fatal(err)
	}
	for idx, b := range readBytes.b {
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
	writer, err := client.NewInodeWriter(ino.Inodeid, testCluster.Leases, testCluster.Names, testCluster.Datas,nil)
	if err != nil {
		t.Fatal(err)
	}
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
		n, err := writer.WriteAt(bytes, uint64(65536*i), uint32(len(bytes)))
		if err != nil {
			t.Fatal(err)
		}
		if n < uint32(len(bytes)) {
			t.Fatal(fmt.Sprintf("Only wrote %d bytes out of %d", n, len(bytes)))
		}
	}
	err = writer.Fsync()
	if err != nil { t.Fatal(err) }
	// then do that many reads across the file to confirm it's ok
	readBytes := &testReadPipe{}
	reader, err := client.NewReader(ino.Inodeid, testCluster.Names, testCluster.Datas)
	if err != nil {
		t.Fatal(fmt.Sprintf("Error opening reader %s", err.Error()))
	}
	for i := 0; i < 5000; i++ {
		err := reader.ReadAt(readBytes, uint64(65536*i), 65536)
		if err != nil {
			t.Fatal(err)
		}
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

	writer, err := client.NewInodeWriter(ino.Inodeid, testCluster.Leases, testCluster.Names, testCluster.Datas,nil)
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

	err = writer.Fsync()
	if err != nil { t.Fatal(err) }
	
	readBytes := &testReadPipe{}
	r, err := client.NewReader(ino.Inodeid, testCluster.Names, testCluster.Datas)
	if err != nil {
		t.Fatal(fmt.Sprintf("Error opening reader %s", err.Error()))
	}
	err = r.ReadAt(readBytes, 0, 5)
	fmt.Printf("Read %d bytes\n", n)
	if err != nil {
		t.Fatal(err)
	}

	for idx := 0; idx < 5; idx++ {
		if readBytes.b[idx] != bytes[idx] {
			t.Fatal(fmt.Sprintf("Bytes not equal at offset %d : %x != %x", idx, readBytes.b[idx], bytes[idx]))
		}
	}
	fmt.Println("Done TestShortRead")
}
