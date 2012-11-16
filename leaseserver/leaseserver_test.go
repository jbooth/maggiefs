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
  server,err := NewLeaseServer()
  if (err != nil) { panic(err) }
  go func() {
    server.Serve()
  }()
  ls,err = NewLeaseClient(fmt.Sprintf("127.0.0.1:%d",DOOZER_PORT))
  if err != nil { panic(err) }
}

func TestReadLease(t *testing.T) {
  o.Do(startDoozer)
  rl,_ := ls.ReadLease(uint64(5))
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
}



