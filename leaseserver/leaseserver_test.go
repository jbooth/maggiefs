package leaseserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"sync"
	"testing"
	"time"
)

var (
	o   sync.Once = sync.Once{}
	ls  maggiefs.LeaseService
	ls2 maggiefs.LeaseService
)

func startServer() {
	fmt.Println("starting lease server")
	server, err := NewLeaseServer(fmt.Sprintf("0.0.0.0:%d", LEASESERVER_PORT))
	if err != nil {
		panic(err)
	}
	go server.Serve()
	fmt.Println("connecting client")
	ls, err = NewLeaseClient(fmt.Sprintf("127.0.0.1:%d", LEASESERVER_PORT))
	if err != nil {
		panic(err)
	}
	ls2, err = NewLeaseClient(fmt.Sprintf("127.0.0.1:%d", LEASESERVER_PORT))
	if err != nil {
		panic(err)
	}
}

func TestReadLease(t *testing.T) {
	o.Do(startServer)
	fmt.Println("getting read lease")
	rl, _ := ls.ReadLease(uint64(5))
	fmt.Println("releasing")
	rl.Release()
}

func TestCommit(t *testing.T) {
	nodeid := uint64(10)
	o.Do(startServer)
	fmt.Printf("testCommit getting readlease\n")
	rl, _ := ls2.ReadLease(nodeid)
	fmt.Printf("got lease %+v, cli id %d\n", rl, 0)
	fmt.Println("asserting no notification so far")
	threeSecondTimeout := time.After(time.Duration(3 * 1e9))
	select {
	case <-ls2.GetNotifier():
		fmt.Println("got event when we shouldn't!")
		t.FailNow()
	case <-threeSecondTimeout:
		// good
		break
	}
	fmt.Println("committing")
	go func() {
		err := ls.Notify(nodeid)
		if err != nil {
			panic(err)
		}
		fmt.Println("done committing")
	}()
	fmt.Println("waiting for notification")
	threeSecondTimeout = time.After(time.Duration(3 * 1e9))
	select {
	case n := <-ls2.GetNotifier():
		fmt.Println("Got notification %+v\n", n)
		n.Ack()
		fmt.Println("Sent Ack")
		break
	case <-threeSecondTimeout:
		fmt.Println("timed out waiting for commit notification!")
		t.Fail()
	}
	fmt.Println("releasing readlease")
	rl.Release()
}

func TestWaitForAck(t *testing.T) {
	nodeid := uint64(10)
	o.Do(startServer)
	fmt.Printf("testCommit getting readlease\n")
	rl, _ := ls2.ReadLease(nodeid)
	fmt.Printf("got lease %+v, cli id %d\n", rl, 0)
	fmt.Println("asserting no notification so far")
	threeSecondTimeout := time.After(time.Duration(3 * 1e9))
	select {
	case <-ls2.GetNotifier():
		fmt.Println("got event when we shouldn't!")
		t.FailNow()
	case <-threeSecondTimeout:
		// good
		break
	}
	fmt.Println("committing")
	notifyDone := make(chan bool)
	go func() {
		err := ls.Notify(nodeid)
		if err != nil {
			panic(err)
		}
		notifyDone <- true
		fmt.Println("done committing")
	}()
	fmt.Println("waiting for notification")
	threeSecondTimeout = time.After(time.Duration(3 * 1e9))
	var n maggiefs.NotifyEvent
	select {
	case n = <-ls2.GetNotifier():
		fmt.Println("Got notification %+v\n", n)
		break
	case <-threeSecondTimeout:
		fmt.Println("timed out waiting for commit notification!")
		t.Fail()
	}
	fmt.Println("testing that Notify didn't return before ack")
	threeSecondTimeout = time.After(time.Duration(3 * 1e9))
	select {
	case <-notifyDone:
		fmt.Println("Notify returned prematurely!")
		t.Fail()
	case <-threeSecondTimeout:
		fmt.Println("timed out waiting for notify to finish before ack, all is well")
		break
	}
	fmt.Println("testing that Notify did return after ack")
	n.Ack()
	threeSecondTimeout = time.After(time.Duration(3 * 1e9))
	select {
	case <-notifyDone:
		fmt.Println("Notify returned after ack ok")
		break
	case <-threeSecondTimeout:
		fmt.Println("notify did NOT return after ack!")
		t.Fail()
	}
	fmt.Println("releasing readlease")
	rl.Release()
}

func TestWaitAllReleased(t *testing.T) {
	nodeid := uint64(23)
	o.Do(startServer)
	fmt.Printf("Getting readlease for testWaitAllReleased\n")
	rl, _ := ls.ReadLease(nodeid)
	allReleasedChan := make(chan bool)
	tenSecondTimeout := time.After(time.Duration(10 * 1e9))
	go func() {
		ls2.WaitAllReleased(nodeid)
		allReleasedChan <- true
	}()
	fmt.Println("asserting that waitAllReleased doesn't succeed while lease holds")
	select {
	case <-allReleasedChan:
		fmt.Println("Wait all released returned early!")
		t.Fail()
	case <-tenSecondTimeout:
		break
	}
	fmt.Println("Asserting that waitAllRelease *does* return after lease is released")
	rl.Release()
	tenSecondTimeout = time.After(time.Duration(10 * 1e9))
	select {
	case <-allReleasedChan:
		fmt.Println("Successfully waited all released")
		break
	case <-tenSecondTimeout:
		fmt.Println("Timed out trying to wait all release!")
		t.Fail()
	}
}

//func TestDisconnect(t *testing.T) {
//	nodeid := uint64(45)
//	// dial new client, we're gonna break it
//	cli, err := NewLeaseClient(fmt.Sprintf("127.0.0.1:%d", LEASESERVER_PORT))

//	o.Do(startServer)
//	_, err = cli.WriteLease(nodeid)
//	if err != nil {
//		panic(err)
//	}
//	expiredChan := make(chan bool)
//	tenSecondTimeout := time.After(time.Duration(10 * 1e9))

//	// this should succeed after client is killed
//	go func() {
//		fmt.Println("Trying to acquire other lease")
//		wl2, err := ls2.WriteLease(nodeid)
//		if err != nil {
//			panic(err)
//		}
//		fmt.Println("Acquired other lease")
//		expiredChan <- true
//		wl2.Release()
//	}()
//	// kill client
//	cli.c.c.Close()
//	fmt.Println("Asserting that killing client drops held leases")
//	select {
//	case <-expiredChan:
//		fmt.Println("Defunct connection successfully expired from lease")
//		break
//	case <-tenSecondTimeout:
//		fmt.Println("Timed out trying to acquire lease!")
//		t.Fail()
//	}
//}
