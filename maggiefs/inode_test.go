package maggiefs

import (
	"testing"
	"reflect"
)

// test some encoding
func TestBlockEncoding(t *testing.T) {
	b := Block{}
	b.Id=123
	b.Version=4
	b.Inodeid=456
	b.StartPos = 1024
	b.EndPos = 2048
	b.Volumes = []uint32{5,6,7}
	
	bytes,_ := b.GobEncode()
	if b.BinSize() != len(bytes) {
		t.Fatalf("binSize %d not equal to len bytes %d",b.BinSize(),len(bytes))
	}
	b2 := Block{}
	b2.GobDecode(bytes)
	
	if ! reflect.DeepEqual(b,b2) {
		t.Fatalf("%+v not equal to %+v\n",b,b2)
	} 
	
}