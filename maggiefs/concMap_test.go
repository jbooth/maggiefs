package maggiefs

import (
"testing"
)

// placefills Lease on the OpenFile type and hacks in a string
type StringLease struct {
  val string
}

func (l StringLease) Release() error {
  return nil
}

func (l StringLease) String() string {
  return l.val
}

func TestConcMap(*testing.T) {
  //m := newOpenFileMap(10)
  for i := uint64(0) ; i < 10 ; i++ {
   

  }

}
