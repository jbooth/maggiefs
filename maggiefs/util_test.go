package maggiefs

import (
  "testing"
  "os"
  "fmt"
)
func TestParseWRFlags(t *testing.T) {
  flag := uint32(os.O_RDONLY)
  readable,_ := parseWRFlags(flag)
  if (readable) { 
    fmt.Println("pass")
  } else {
    fmt.Println("fail")
    t.Fail()
  }
  flag = uint32(os.O_RDWR)
  readable,writable := parseWRFlags(flag)
  if (readable && writable) {
    fmt.Println("pass\n")
  } else {
    t.Fail()
  }
}
