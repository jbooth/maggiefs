package maggiefs

const (
  FTYPE_DIR = int(0)
  FTYPE_REG = int(1)
  FTYPE_LNK = int(2)
)

type Inode struct {
  Inodeid uint64
  Generation uint64
  Ftype int
  Length uint64
  Mode uint32
  Mtime int64 // changed on data - can be changed by user with touch
  Ctime int64 // changed on file attr change or date -- owned by kernel
  Nlink uint32 // number of paths linked to this inode
  Uid uint32
  Gid uint32
  Blocks []Block // can be 0 blocks in case of directory or empty file
  Children map[string] uint64 // empty unless we are a dir, maps name to inode id 
}

type Block struct {
  StartPos uint64
  EndPos uint64
  Locations []string
}


