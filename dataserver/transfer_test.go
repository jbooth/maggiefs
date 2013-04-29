package dataserver

import (
  "os"
  "io/ioutil"
  "syscall"
  "crypto/rand"
  "testing"
  "fmt"
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
  f.Write(data)
  
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

func TestSplice(t *testing.T) {
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