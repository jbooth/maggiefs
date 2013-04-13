package leaseserver

import (

  "github.com/jbooth/maggiefs/maggiefs"
  "testing"
  "fmt"
  "sync"
  "time"
)

var (
  o sync.Once = sync.Once{}
  ls maggiefs.LeaseService
  ls2 maggiefs.LeaseService
)

func startServer() {
  fmt.Println("starting lease server")
  server,err := NewLeaseServer(fmt.Sprintf("0.0.0.0:%d",LEASESERVER_PORT))
  if (err != nil) { panic(err) }
  go func() {
    server.Serve()
  }()
  fmt.Println("connecting client")
  ls,err = NewLeaseClient(fmt.Sprintf("127.0.0.1:%d",LEASESERVER_PORT))
  if err != nil { panic(err) }
  ls2,err = NewLeaseClient(fmt.Sprintf("127.0.0.1:%d",LEASESERVER_PORT))
  if err != nil { panic(err) }
}

func TestReadLease(t *testing.T) {
  o.Do(startServer)
  fmt.Println("getting read lease")
  rl,_ := ls.ReadLease(uint64(5))
  fmt.Println("releasing")
  rl.Release()
}

func TestCommit(t *testing.T) {
  nodeid := uint64(10)
  o.Do(startServer)
  rl,_ := ls2.ReadLease(nodeid)
  fmt.Printf("got lease %+v, cli id %d\n",rl,0)
  wl,_ := ls.WriteLease(nodeid)
  
  fmt.Printf("got lease %+v, cli id %d\n",wl,0)
  fmt.Println("asserting no notification so far")
  threeSecondTimeout := time.After(time.Duration(3*1e9))
  select {
    case <- ls2.GetNotifier():
      fmt.Println("got event when we shouldn't!")
      t.FailNow()
    case <- threeSecondTimeout:
      // good
      break
  }
  fmt.Println("committing")
  err := wl.Release()
  if err != nil { panic(err) }
  fmt.Println("waiting for notification")
  threeSecondTimeout = time.After(time.Duration(3*1e9))
  select {
    case <- ls2.GetNotifier():
      break
    case <- threeSecondTimeout:
      fmt.Println("timed out waiting for commit notification!")
      t.Fail()
  }
  rl.Release()
}

func TestWriteLeaseContention(t *testing.T) {
  nodeid := uint64(12)
  o.Do(startServer)
  
  wl1,_ := ls.WriteLease(nodeid)
  fmt.Printf("got lease %+v\n",wl1)
  
  successChan := make(chan bool)
  tenSecondTimeout := time.After(time.Duration(10*1e9))
  
  go func() {
    wl2,err := ls2.WriteLease(nodeid)
    if err != nil { t.Fail() }
    successChan <- true
    wl2.Release()
  }()
  fmt.Printf("asserting we can't get writeLease while held")
  select {
    case <- successChan:
      fmt.Println("Got 2 writeleases on same node simultaneously!")
      t.Fail()
    case <- tenSecondTimeout:
      break
  }
  
  fmt.Println("Releasing, asserting we succeed after")
  wl1.Release()
  tenSecondTimeout = time.After(time.Duration(10*1e9))
  select {
    case <- successChan:
      fmt.Println("Successfully acquired!")
      break
    case <- tenSecondTimeout:
      fmt.Println("Timed out trying to get writeLease uncontested!")
      t.Fail()
  }
  
}

func TestWaitAllReleased(t *testing.T) {
	nodeid := uint64(23)
	o.Do(startServer)
	
	rl,_ := ls.ReadLease(nodeid)
	allReleasedChan := make(chan bool)
	tenSecondTimeout := time.After(time.Duration(10*1e9))
	go func() {
		ls2.WaitAllReleased(nodeid)
		allReleasedChan <- true
	}()
	fmt.Println("asserting that waitAllReleased doesn't succeed while lease holds")
	  select {
    case <- allReleasedChan:
      fmt.Println("Wait all released returned early!")
      t.Fail()
    case <- tenSecondTimeout:
      break
  }
  fmt.Println("Asserting that waitAllRelease *does* return after lease is released")
  rl.Release()
  tenSecondTimeout = time.After(time.Duration(10*1e9))
    select {
    case <- allReleasedChan:
      fmt.Println("Successfully waited all released")
      break
    case <- tenSecondTimeout:
      fmt.Println("Timed out trying to wait all release!")
      t.Fail()
  }
}



