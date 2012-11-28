package leaseserver

import (
	"fmt"
	"strconv"
	"github.com/4ad/doozer"
	"github.com/jbooth/maggiefs/maggiefs"
	"sync"
	"strings"
)

const(
//  CREATE_BODY byte[] = {1,2}
//  COMMIT_BODY = []byte{2}
)

type LeaseClient struct {
	doozer    *doozer.Conn
	leaseBody []byte
	notifier chan uint64
}

func (lc LeaseClient) WriteLease(nodeid uint64) (l maggiefs.WriteLease, err error) {
	path := leasePath(nodeid) + "/w"
	success := false
	nodeRev,err := lc.doozer.Rev()
	if err != nil { return nil,err }
	// keep trying until we're successful
	for !success {
		// check if lock exists
		_, nodeRev, err := lc.doozer.Get(path, nil)
		if err != nil {
			return nil, err
		}

		if nodeRev != 0 {
			// if node existed, wait till its deleted
			deleted := false
			for !deleted {
				// if so, wait till it clears
				ev, err := lc.doozer.Wait(path, nodeRev)
				if err != nil {
					return nil, err
				}
				if ev.IsDel() { // make sure this was a del event and not a commit
					deleted = true
					nodeRev = ev.Rev
				}
			}
		}

		// try to claim
		newRev, err := lc.doozer.Set(path, nodeRev, lc.leaseBody)
		if err != nil { panic(err) }
		if newRev != 0 {
			success = true
			nodeRev = newRev
		} // else we re-loop and wait again
	}
	// successfully claimed, wrap the lease around it and return
	return writeLease{
		doozer:    lc.doozer,
		m:         new(sync.Mutex),
		leasePath: path,
		leaseRev:  nodeRev + 1,
		inodeid:   nodeid,
	}, nil
}

func (lc LeaseClient) ReadLease(nodeid uint64) (l maggiefs.ReadLease, err error) {
//	// ensure parent exists
	// add read lease node
	// we can have multiple readleases so need to do autoincrementing child
  parentPath := leasePath(nodeid) + "/r"
	var leasePath string
	success := false
	for !success {
	 
	 highestInt := uint64(1)
   _,parentRev,err := lc.doozer.Stat(parentPath,nil)
   if (parentRev != 0) {
     // if we have existing children, find the highest int name
     children,err := lc.doozer.Getdir(parentPath,parentRev,0,-1)
     if (err != nil) { return nil,err }
     for _,v := range children {
       thisInt,err := strconv.ParseUint(v,10,64)
       if (err != nil) { return nil,err }
       if (thisInt > highestInt) { highestInt = thisInt }
     }
   }
   // the path we're going to create is 1 higher than any that exist
	 leasePath = parentPath + "/" + strconv.FormatUint(highestInt + 1,10)
	 leaseRev,err := lc.doozer.Set(leasePath,0,[]byte{})
	 // err old rev means someone else got to this node first, need to try again
	 if err != doozer.ErrOldRev {
	   // check if it's another error
	   if err != nil { return nil,err }
	   // if we succeeded in setting this node, return!
	   return readLease {lc.doozer,leasePath,leaseRev},nil
	 }
	 // else we re-loop and try again
	} 
	// shouldn't happen
	return nil, nil
}

func (lc LeaseClient) WaitAllReleased(nodeid uint64) error {
	// first list children
	leasePath := leasePath(nodeid) + "/r"
	stat,err := lc.doozer.Statinfo(0,leasePath)
	if err != nil { return err }
	// if none outstanding, return right away
	if !stat.IsDir { return nil }
	leaseRev := stat.Rev
	
	success := false
	
	// now wait until all are destroyed
	for !success {
		children,err := lc.doozer.Getdir(leasePath,stat.Rev,0,-1)
		if (err != nil) { return err }
		if len(children) == 0 { 
		  success = false //break
		} else {
		  ev,err := lc.doozer.Wait(leasePath+"/*",leaseRev)
		  if err != nil { return err }
		  leaseRev = ev.Rev
		}
	}
	return nil
}

func (lc LeaseClient) GetNotifier() chan uint64 {
	return lc.notifier
}

// TODO handle errors here
func (lc LeaseClient) GoNotify() {
  rev,err := lc.doozer.Rev()
  if err != nil { panic(err) }
  
  for {
    
    ev,err := lc.doozer.Wait("/leases/*/w",rev)
    if err != nil { panic(err) }
    rev = ev.Rev
    splits := strings.Split(ev.Path,"/")
    
    inodeId,err := strconv.ParseUint(splits[2],10,64)
    if err != nil { panic(err) }
    lc.notifier <- inodeId
  }
}


// gets the path for this node from doozer
func leasePath(nodeid uint64) string {
	return fmt.Sprintf("/leases/%d", nodeid)
}

func NewLeaseClient(doozerHost string) (maggiefs.LeaseService,error) {
  doozer,err := doozer.Dial(doozerHost)
  if (err != nil) { return nil,err }
	ret := LeaseClient{doozer, []byte{}, make(chan uint64)}
	go ret.GoNotify()
	return ret,nil
}
 