package leaseserver

import (
  "fmt"
  "github.com/jbooth/maggiefs/maggiefs"
  "github.com/4ad/doozer"
)

type LeaseClient struct {
  doozer *doozer.Conn
  leaseBody []byte
  
}

func (lc LeaseClient) WriteLease(nodeid uint64) (l maggiefs.WriteLease, err error) {
  path := leasePath(nodeid) + "/w"
  success := false
  // keep trying until we're successful
  for ; !success ; {
    // check if lock exists
    _,prevRev,err := lc.doozer.Get(path,nil)
    if (err != nil) { return nil,err }
    for ; prevRev != 0 ; {
      // if so, wait till it clears
      ev,err := lc.doozer.Wait(path,prevRev)
      if (err != nil) { return nil,err }
      prevRev = ev.Rev
    } 
    
    // try to claim
    newRev,err := lc.doozer.Set(path,0,lc.leaseBody)
    if (newRev != 0) { 
      success = true 
    } // else we re-loop and wait again
  }
  return nil,nil
}

func (lc LeaseClient) ReadLease(nodeid uint64, notifier chan maggiefs.ChangeNotify) (l maggiefs.ReadLease, err error) {
  return nil,nil
}

func (lc LeaseClient) WaitAllReleased(nodeid uint64) error {
  return nil
}

// gets the path for this node from doozer
func leasePath(nodeid uint64) string {
  return fmt.Sprintf("/leases/%d",nodeid)  
}



func NewLeaseClient() maggiefs.LeaseService {
  // make sure the base lease path exists
  
  return LeaseClient{}
}

