package maggiefs

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
  Mtime int64 // changed on data - can be changed by user with touch
  Ctime int64 // changed on file attr change or date -- owned by kernel
  Nlink uint32 // number of paths linked to this inode
  Uid uint32
  Gid uint32
  Symlinkdest string // only populated for symlinks, "" otherwise
  Blocks []Block // can be 0 blocks in case of directory,symlink or empty file
  Children map[string] uint64 // empty unless we are a dir, maps name to inode id 
  Xattr map[string] []byte
}

func (i Inode) IsDir() bool {
  return i.Ftype == FTYPE_DIR
}

type Block struct {
  Id uint64 // globally unique block id
  Mtime uint64 // last modified
  Inodeid uint64
  StartPos uint64
  EndPos uint64
  Locations []string
}


