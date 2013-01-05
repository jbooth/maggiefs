package maggiefs

import (
  "fmt"
  "syscall"
)

const (
  FTYPE_DIR = int(0)
  FTYPE_REG = int(1)
  FTYPE_LNK = int(2)
  PAGESIZE = uint32(4096)
  BLOCKLENGTH = uint64(1024 * 1024 * 128) // 128MB, hardcoded for now
)

type Inode struct {
  Inodeid uint64
  Generation uint64
  Ftype int
  Length uint64
  Mode uint32
  Atime int64
  Mtime int64 // changed on data - can be changed by user with touch
  Ctime int64 // changed on file attr change or date -- owned by kernel
  Nlink uint32 // number of paths linked to this inode
  Uid uint32
  Gid uint32
  Symlinkdest string // only populated for symlinks, "" otherwise
  Blocks []Block // can be 0 blocks in case of directory,symlink or empty file
  Children map[string] Dentry // empty unless we are a dir, maps name to inode id 
  Xattr map[string] []byte
}

func (i *Inode) String() string {
  return fmt.Sprintf(
    "Inode { InodeId: %d, Length %d Ftype %d Mode %o Mtime %d Blocks %+v Children %v}",
    i.Inodeid,
    i.Length,
    i.Ftype,
    i.Mode,
    i.Mtime,
    i.Blocks,
    i.Children)
}

// all 0777 for now
func (i *Inode) FullMode() uint32 {
  switch {
    case FTYPE_DIR == i.Ftype:
      return syscall.S_IFDIR | 0777
    case FTYPE_REG == i.Ftype:
      return syscall.S_IFREG | 0777
    case FTYPE_LNK == i.Ftype:
      return syscall.S_IFLNK | 0777
  }
  return syscall.S_IFREG | 0777
}
type Dentry struct {
  Inodeid uint64
  CreatedTime int64 // time this link was created.  used to return consistent ordering in ReadDir.
}

func (i *Inode) IsDir() bool {
  return i.Ftype == FTYPE_DIR
}

type Block struct {
  Id uint64 // globally unique block id
  Mtime int64 // last modified
  Generation uint64 // mod number
  Inodeid uint64 // which inode we belong to
  StartPos uint64 // alignment of this bytes first block in the file
  EndPos uint64 // alignment of this bytes last block in the file 
  Volumes []int32 // IDs for the volumes we're replicated over
}

func (b *Block) Length() uint64 {
  return b.EndPos - b.StartPos
}

type DataNodeStat struct {
  Dnid int32
  Size uint64 // total bytes
  Used uint64 // bytes used
  Free uint64 // bytes free
  Volumes []VolumeStat
}

type VolumeStat struct {
  Id int32
  DnId int32
  Size uint64 // total bytes
  Used uint64 // bytes used
  Free uint64 // bytes free
  NumBlocks uint64
}

