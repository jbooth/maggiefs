package maggiefs

import (
	"reflect"
	"testing"
)

// test some encoding
func TestBlockEncoding(t *testing.T) {
	b := Block{}
	b.Id = 123
	b.Version = 4
	b.Inodeid = 456
	b.StartPos = 1024
	b.EndPos = 2048
	b.Volumes = []uint32{5, 6, 7}

	bs, _ := b.GobEncode()
	if b.BinSize() != len(bs) {
		t.Fatalf("binSize %d not equal to len bytes %d", b.BinSize(), len(bs))
	}
	b2 := Block{}
	b2.GobDecode(bs)

	if !reflect.DeepEqual(b, b2) {
		t.Fatalf("%+v not equal to %+v\n", b, b2)
	}

}

func TestInodeEncoding(t *testing.T) {
	i := Inode{}
	i.Inodeid = 23
	i.Generation = 1
	i.Mode = 0777
	i.Ftype = FTYPE_DIR
	i.Blocks = make([]Block, 2, 2)
	i.Blocks[0] = Block{123, 4, 456, 1024, 1024, []uint32{5, 6, 7}}
	i.Blocks[1] = Block{125, 4, 456, 2048, 4096, []uint32{5, 6, 7}}
	i.Children = make(map[string]Dentry)
	i.Children["child1"] = Dentry{56, 1234}
	i.Children["child2"] = Dentry{67, 5678}

	bytes, _ := i.GobEncode()
	if i.BinSize() != len(bytes) {
		t.Fatalf("binSize %d not equal to len bytes %d", i.BinSize(), len(bytes))
	}
	i2 := Inode{}
	i2.GobDecode(bytes)
	if !reflect.DeepEqual(i, i2) {
		t.Fatalf("%+v not equal to %+v\n", i, i2)
	}
}

func TestStringWrap(t *testing.T) {
	s := []byte{45, 46, 47, 48}

	str := string(s)
	s[2] = 49
	str2 := string(s)
	if str == str2 {
		t.Fatalf("strings should not be equal %s %s", str, str2)
	}
}
