package maggiefs

import (
  "testing"
  "fmt"
  "sync/atomic"
)

func TestIncrementAndGet (t *testing.T) {
  x := uint64(0)
  for i := 0 ; i < 10 ; i++ {
    oldX := atomic.LoadUint64(&x)
    x = IncrementAndGet(&x,uint64(i))
    fmt.Printf("oldX: %d i: %d newX: %d\n",oldX,i,x)
    if x != oldX+uint64(i) {
      t.Errorf("x+i != x.IncrementAndGet(i)")
    }
  }
}
