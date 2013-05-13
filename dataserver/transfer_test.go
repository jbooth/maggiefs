package dataserver

import (
  "os"
  "io/ioutil"
  "syscall"
  "crypto/rand"
  "testing"
  "fmt"
  "sync"
)

func TestSendfile(t *testing.T) {
  f,err := ioutil.TempFile("/tmp","testSendFile")
  if err != nil {
    t.Fatal(err)
  }
  defer os.Remove(fmt.Sprintf("/tmp/%s",f.Name()))
  
  sock1,sock2,err := socketPair()
  if err != nil {
    t.Fatal(err)
  }
  // 10mb random data
  data := make([]byte,1024*1024*2)
  rand.Read(data)
  _,err = f.Write(data)
  if err != nil {
  	t.Fatal(err)
  }
  go func() {
    SendFile(f,sock1,0,len(data))  
  }()
  data2 := make([]byte,1024*1024*2)
  numRead := 0
  for ; numRead < len(data2) ; {
	  n,err := sock2.Read(data2[numRead:])
    if err != nil {
      t.Fatal(err)
    }
    numRead += n
  }
  for idx,b := range data {
    if b != data2[idx] {
      t.Fatalf("bytes not equal at idx %d : %d != %d",idx,b,data2[idx])
    }
  }
  return
}

// we splice from a socket to a file, while teeing to another socket, then validate all bytes after
func TestSpliceAndTee(t *testing.T) {
	// socketpair heading into splice
	inRemote,inLocal,err := socketPair()
	if err != nil {
		t.Fatal(err.Error())
	}
	// file heading out of splice
	
	f,err := ioutil.TempFile("/tmp","splice")
	if err != nil {
		t.Fatal(err.Error())
	}
	// socketpair heading out of tee
	outLocal,outRemote,err := socketPair()
	if err != nil {
		t.Fatal(err.Error())
	}
	
	// gen random bytes
	bytesIn := make([]byte,1024*1024*2)
	bytesOut := make([]byte,1024*1024*2)
	_,_ = rand.Read(bytesIn)
	wg := &sync.WaitGroup{}
	wg.Add(3)
	// write to inSocket, while splicing to file and teeSocket, and reading results for comparison
  go func() {
  	defer wg.Done()
  	fmt.Println("Writing to remote")
  	// write bytes to inRemote
  	nWritten := 0
  	for ; nWritten < len(bytesIn) ; {
  		n,err := inRemote.Write(bytesIn[nWritten:])
  		fmt.Printf("Wrote %d to inRemote\n",n)
  		if err != nil {
  			panic(err)
  		}
  		nWritten += n
  	}
  }()
  
  go func() {
  	defer wg.Done()
  	fmt.Println("Splicing")
		// splice from inLocal to file f and socket outLocal 
  	//func SpliceAdv(in *os.File, inOff *int64, out *os.File, outOff *int64, teeFiles []*os.File, length int) 
  	teeFiles := []*os.File{outLocal}
  	fmt.Println("Splicing")
  	err := SpliceAdv(inLocal,nil,f,nil,teeFiles,len(bytesIn))
  	fmt.Println("Splice done")
  	if err != nil {
  		panic(err)
  	}
  }()
  
  go func() {
  	defer wg.Done()
  	fmt.Println("Reading from outRemote")
  	// read from outRemote
  	nRead := 0
  	for ; nRead < len(bytesOut) ; {
  		n,err := outRemote.Read(bytesOut[nRead:])
  		fmt.Printf("Read %d from outRemote\n",n)
  		if err != nil {
  			panic(err)
  		}
  		nRead += n
  	}
  }()
  
  // check results
  wg.Wait()
  fmt.Println("Finished!")
  
  return
}

func socketPair() (sock1 *os.File, sock2 *os.File, err error) {
  var fds [2]int
  fds,err = syscall.Socketpair(syscall.AF_LOCAL, syscall.SOCK_STREAM, 0)
  if err != nil {
    fmt.Println(err.Error())
  }
  return os.NewFile(uintptr(fds[0]),"|0"),os.NewFile(uintptr(fds[1]),"|1"),err
}