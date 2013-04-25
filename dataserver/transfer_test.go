package dataserver

import (
  "net"
  "os"
  "io/ioutil"
)

func TestSendfile(t *testing.T) {
  f,err := ioutil.TempFile("/tmp","testSendFile")
  if err != nil {
    t.Fail(err)
  }
  defer os.Remove(fmt.Sprintf("/tmp/%s"f.Name()))
  listener,err := net.Listen("tcp4","0.0.0.0:1234")
  
  var transferToSock *net.TCPConn
  var listeningSock *net.TCPConn
  
  sockCreatedChan := make(chan error)
  go func() {
    
  }
  
  
  return
}

