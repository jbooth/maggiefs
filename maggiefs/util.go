package maggiefs

import (
	"path/filepath"
	"fmt"
	"strings"
)

// deep copies an inode
func CopyInode(i *Inode) *Inode {
	ret := Inode{}
	ret.Inodeid = i.Inodeid
	ret.Generation = i.Generation
	ret.Ftype = i.Ftype
	ret.Length = i.Length
	ret.Mode = i.Mode
	ret.Mtime = i.Mtime
	ret.Ctime = i.Ctime
	ret.Nlink = i.Nlink
	ret.Uid = i.Uid
	ret.Gid = i.Gid
	ret.Symlinkdest = i.Symlinkdest
	ret.Children = make(map[string]Dentry)
	ret.Xattr = make(map[string][]byte)
	ret.Blocks = make([]Block, len(i.Blocks), len(i.Blocks))
	for i, b := range i.Blocks {
		ret.Blocks[i] = b
	}
	for n, c := range i.Children {
		ret.Children[n] = c
	}
	for n, v := range i.Xattr {
		newV := make([]byte, len(v), len(v))
		copy(newV, v)
		ret.Xattr[n] = newV
	}
	return &ret
}

func ResolveInode(path string, ns NameService) (*Inode,error) {
	if filepath.IsAbs(path) {
		return nil,fmt.Errorf("Path %s is absolute! Must be relative to mountpoint!",path)
	}
	pathComponents := strings.Split(path,"/")
	inodeId := uint64(ROOT_INO)
	for _,pathChunk := range pathComponents {
		if pathChunk == "" {  // empty chunks from double slash
			continue
		}
		ino,err := ns.GetInode(inodeId)
		if err != nil {
			return nil,err
		}
		dentry,ex := ino.Children[pathChunk]
		if !ex {
			return nil,fmt.Errorf("Inode %d has no child %s",inodeId,pathChunk)
		}
		inodeId = dentry.Inodeid
	}
	return ns.GetInode(inodeId)
}

func BlocksInRange(blocks []Block, start uint64, length uint64) []Block {
	// scan once to figure out how many
	numBlocksInRange := 0
	end := start + length
	for _,b := range blocks {
		if (b.StartPos <= start && b.EndPos > start) || (b.StartPos <= end) {
			numBlocksInRange++
		}
	}
	// scan again and add
	ret := make([]Block,numBlocksInRange)
	idx := 0
	for _,b := range blocks {
		if (b.StartPos <= start && b.EndPos > start) || (b.StartPos <= end) {
			ret[idx] = b
			idx++
		}
	}
	return ret
}


