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
)

func startDoozer() {
  fmt.Println("starting lease server")
  server,err := NewLeaseServer(LEASESERVER_PORT)
  if (err != nil) { panic(err) }
  go func() {
    server.Serve()
  }()
  fmt.Println("connecting client")
  ls,err = NewLeaseClient(fmt.Sprintf("127.0.0.1:%d",LEASESERVER_PORT))
  if err != nil { panic(err) }
}

func TestReadLease(t *testing.T) {
  o.Do(startDoozer)
  fmt.Println("getting read lease")
  rl,_ := ls.ReadLease(uint64(5))
  fmt.Println("releasing")
  rl.Release()
}

func TestCommit(t *testing.T) {
  nodeid := uint64(10)
  o.Do(startDoozer)
  wl,_ := ls.WriteLease(nodeid)
  fmt.Printf("got lease %+v\n",wl)
  fmt.Println("asserting no notification so far")
  threeSecondTimeout := time.After(time.Duration(3*1e9))
  select {
    case <- ls.GetNotifier():
      fmt.Println("got event when we shouldn't!")
      t.FailNow()
    case <- threeSecondTimeout:
      // good
      break
  }
  fmt.Println("committing")
  err := wl.Commit()
  if err != nil { panic(err) }
  fmt.Println("waiting for notification")
  threeSecondTimeout = time.After(time.Duration(3*1e9))
  select {
    case <- ls.GetNotifier():
      break
    case <- threeSecondTimeout:
      fmt.Println("timed out waiting for commit notification!")
      t.Fail()
  }
  wl.Release()
}

func TestWait(t *testing.T) {
  nodeid := uint64(12)
  o.Do(startDoozer)
  
  wl1,_ := ls.WriteLease(nodeid)
  fmt.Printf("got lease %+v\n",wl1)
  
  successChan := make(chan bool)
  tenSecondTimeout := time.After(time.Duration(10*1e9))
  
  go func() {
    wl2,err := ls.WriteLease(nodeid)
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
      break
    case <- tenSecondTimeout:
      fmt.Println("Timed out trying to get writeLease uncontested!")
      t.Fail()
  }
  
}



