package maggiefs

const (
  FTYPE_DIR = int(0)
  FTYPE_REG = int(1)
  FTYPE_LNK = int(2)
)

type PathEntry struct {
  Path string
  Inodeid uint64
  Ftype int
  SymlinkDest string
  Uid uint32
  Gid uint32
}

type Inode struct {
  Inodeid uint64
  Generation uint64
  Ftype int
  Length uint64
  Mtime uint64 // changed on data - can be changed by user with touch
  Ctime uint64 // changed on file attr change or date -- owned by kernel
  Nlink uint32 // number of paths linked to this inode
  Uid uint32
  Gid uint32
  Blocks []Block
  Refcount uint32
}

type Block struct {
  Inodeid uint64
  FirstPage uint64
  LastPage uint64
  Locations []string
}
