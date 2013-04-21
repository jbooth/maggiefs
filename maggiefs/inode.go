package maggiefs

import (
  "syscall"
  "fmt"
  "encoding/binary"
  "bytes"
)


const (
  FTYPE_DIR   = uint32(0)
  FTYPE_REG   = uint32(1)
  FTYPE_LNK   = uint32(2)
  PAGESIZE    = uint32(4096)
  BLOCKLENGTH = uint64(1024 * 1024 * 128) // 128MB, hardcoded for now
)

type Inode struct {
  Inodeid     uint64
  Generation  uint64
  Ftype       uint32
  Length      uint64
  Mode        uint32
  Atime       int64
  Mtime       int64  // changed on data change - can be changed by user with touch
  Ctime       int64  // changed on file attr change or date -- owned by kernel
  Nlink       uint32 // number of paths linked to this inode
  Uid         uint32
  Gid         uint32
  Symlinkdest string            // only populated for symlinks, nil or "" otherwise
  Blocks      []Block           // can be 0 blocks in case of directory,symlink or empty file
  Children    map[string]Dentry // empty unless we are a dir, maps name to inode id 
  Xattr       map[string][]byte
}

func (i *Inode) BinSize() int {
	// first items are 
	
	return 0
}

func (i *Inode) GobEncode() ([]byte,error) {
	return nil,nil
}

func (i *Inode) GobDecode(bytes []byte) {
	return
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

func (i *Inode) ToBytes() []byte {
  if i == nil {
    return []byte{}
  }
  ret := make([]byte, binary.Size(*i))
  binary.Write(bytes.NewBuffer(ret), binary.LittleEndian, i)
  return ret
}

func (i *Inode) FromBytes(b []byte) {
  binary.Read(bytes.NewBuffer(b), binary.LittleEndian, i)
}



type Dentry struct {
  Inodeid     uint64
  CreatedTime int64 // time this link was created.  used to return consistent ordering in ReadDir.
}

func (i *Inode) IsDir() bool {
  return i.Ftype == FTYPE_DIR
}

type Block struct {
  Id         uint64  // globally unique block id
  Version		 uint64  // revision number
  Inodeid    uint64  // which inode we belong to
  StartPos   uint64  // alignment of this bytes first block in the file
  EndPos     uint64  // alignment of this bytes last block in the file 
  Volumes    []uint32 // IDs for the volumes we're replicated over -- 0 is an invalid volume
}

func (b Block) BinSize() int {
	// 40 for 5 longs
	size := 40
	// num volumes encoded as a single byte lol
	size++
	// now 4 for each int
	size += (4*len(b.Volumes))
	return size
}

func (b *Block) GobEncode() ([]byte,error) {
	size := b.BinSize()
	bytes := make([]byte,size,size)
	currOff := 0
	// write uint64s
	binary.LittleEndian.PutUint64(bytes[currOff:],b.Id)
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:],b.Version)
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:],b.Inodeid)
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:],b.StartPos)
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:],b.EndPos)
	currOff += 8
	// write num volumes and then volumes
	bytes[currOff] = byte(len(b.Volumes))
	currOff++
	for _,v := range b.Volumes {
		binary.LittleEndian.PutUint32(bytes[currOff:],v)
		currOff += 4
	}
	return bytes,nil
}

func (b *Block) GobDecode(bytes []byte) {
	currOff := 0
	b.Id = binary.LittleEndian.Uint64(bytes[currOff:])
	currOff += 8
	b.Version = binary.LittleEndian.Uint64(bytes[currOff:])
	currOff += 8
	b.Inodeid = binary.LittleEndian.Uint64(bytes[currOff:])
	currOff += 8
	b.StartPos = binary.LittleEndian.Uint64(bytes[currOff:])
	currOff += 8
	b.EndPos = binary.LittleEndian.Uint64(bytes[currOff:])
	currOff += 8
	numVolumes := bytes[currOff]
	currOff++
	b.Volumes = make([]uint32,numVolumes,numVolumes)
	for i := byte(0) ; i < numVolumes ; i++ {
		b.Volumes[i] = binary.LittleEndian.Uint32(bytes[currOff:])
		currOff += 4
	}
}


func (b *Block) Length() uint64 {
  return b.EndPos - b.StartPos
}

func(b *Block) ToBytes() []byte {
  ret := make([]byte, binary.Size(b))
  binary.Write(bytes.NewBuffer(ret), binary.LittleEndian, b)
  return ret
}

func (b *Block) FromBytes(bin []byte) {
  binary.Read(bytes.NewBuffer(bin), binary.LittleEndian,b)
}
