package maggiefs

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"syscall"
	"time"
)

const (
	FTYPE_DIR   = uint32(0)
	FTYPE_REG   = uint32(1)
	FTYPE_LNK   = uint32(2)
	PAGESIZE    = uint32(4096)
	BLOCKLENGTH = uint64(1024 * 1024 * 128) // 128MB, hardcoded for now
	ROOT_INO    = uint64(1)                 // root ino for the filesystem
)

func init() {
	ino := Inode{}
	gob.RegisterName("Inode", &ino)
	gob.RegisterName("Block", Block{})
}

type Inode struct {
	Inodeid     uint64
	Generation  uint64
	Ftype       uint32
	Length      uint64
	Mode        uint32
	Mtime       int64  // changed on data change - can be changed by user with touch
	Ctime       int64  // changed on file attr change or date -- owned by kernel
	Nlink       uint32 // number of paths linked to this inode
	Uid         uint32
	Gid         uint32
	Symlinkdest string            // only populated for symlinks, nil or "" otherwise
	Blocks      []Block           // can be 0 blocks in case of directory,symlink or empty file
	Children    map[string]Dentry // empty unless we are a dir, maps name to inode id
}

func NewInode(id uint64, ftype, mode, uid, gid uint32) *Inode {
	currTime := time.Now().Unix()
	return &Inode{
		id,
		0,
		ftype,
		0,
		mode,
		currTime,
		currTime,
		0,
		0,
		0,
		"",
		make([]Block, 0, 0),
		make(map[string]Dentry),
	}
}

func (i *Inode) BinSize() int {
	// first ints are 8+8+4+8+4+8+8+4+4+4 = 60
	size := 60
	size += 2
	size += len(i.Symlinkdest)

	// blocks encoded as uint32 numBlocks, followed by each block
	size += 4
	for _, b := range i.Blocks {
		size += b.BinSize()
	}

	// children encoded as uint32 numChildren, followed by:
	// uint16 string length, string bytes
	// 16 bytes for dentry
	size += 4
	for name, _ := range i.Children {
		// 2 byte for name length, 16 for dentry, N for actual name bytes
		size += 18 + len(name)
	}
	return size
}

// does not bounds check make sure you allocate enough space using BinSize()
// returns num written
func (i *Inode) ToBytes(bytes []byte) int {
	currOff := 0
	binary.LittleEndian.PutUint64(bytes[currOff:], i.Inodeid)
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:], i.Generation)
	currOff += 8
	binary.LittleEndian.PutUint32(bytes[currOff:], i.Ftype)
	currOff += 4
	binary.LittleEndian.PutUint64(bytes[currOff:], i.Length)
	currOff += 8
	binary.LittleEndian.PutUint32(bytes[currOff:], i.Mode)
	currOff += 4
	binary.LittleEndian.PutUint64(bytes[currOff:], uint64(i.Mtime))
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:], uint64(i.Ctime))
	currOff += 8
	binary.LittleEndian.PutUint32(bytes[currOff:], i.Nlink)
	currOff += 4
	binary.LittleEndian.PutUint32(bytes[currOff:], i.Uid)
	currOff += 4
	binary.LittleEndian.PutUint32(bytes[currOff:], i.Gid)
	currOff += 4
	// symlink dest encoded as uint16 + string bytes
	length := len(i.Symlinkdest)
	binary.LittleEndian.PutUint16(bytes[currOff:], uint16(length))
	currOff += 2
	copy(bytes[currOff:], i.Symlinkdest)
	currOff += length
	// blocks encoded as uint32 numBlocks + each block in a row
	numBlocks := uint32(len(i.Blocks))
	binary.LittleEndian.PutUint32(bytes[currOff:], numBlocks)
	currOff += 4
	// now each block is encoded in a line -- we use binsize to handle our offsets
	for _, b := range i.Blocks {
		currOff += b.ToBytes(bytes[currOff:])
	}

	// now we do dentries -- N pairs of string and dentry
	if i.Children == nil {
		i.Children = make(map[string]Dentry)
	}
	numDentries := uint32(len(i.Children))
	binary.LittleEndian.PutUint32(bytes[currOff:], numDentries)
	currOff += 4
	for name, dentry := range i.Children {
		nameLen := len(name)
		binary.LittleEndian.PutUint16(bytes[currOff:], uint16(nameLen))
		currOff += 2
		copy(bytes[currOff:], name)
		currOff += nameLen
		binary.LittleEndian.PutUint64(bytes[currOff:], dentry.Inodeid)
		currOff += 8
		binary.LittleEndian.PutUint64(bytes[currOff:], uint64(dentry.CreatedTime))
		currOff += 8
	}
	return currOff
}

// reads from the bytes, returns num read
// TODO this keeps the underlying buffer around for a while..  might actually be a benefit?
func (i *Inode) FromBytes(bytes []byte) int {
	currOff := 0
	i.Inodeid = binary.LittleEndian.Uint64(bytes[currOff:])
	currOff += 8
	i.Generation = binary.LittleEndian.Uint64(bytes[currOff:])
	currOff += 8
	i.Ftype = binary.LittleEndian.Uint32(bytes[currOff:])
	currOff += 4
	i.Length = binary.LittleEndian.Uint64(bytes[currOff:])
	currOff += 8
	i.Mode = binary.LittleEndian.Uint32(bytes[currOff:])
	currOff += 4
	i.Mtime = int64(binary.LittleEndian.Uint64(bytes[currOff:]))
	currOff += 8
	i.Ctime = int64(binary.LittleEndian.Uint64(bytes[currOff:]))
	currOff += 8
	i.Nlink = binary.LittleEndian.Uint32(bytes[currOff:])
	currOff += 4
	i.Uid = binary.LittleEndian.Uint32(bytes[currOff:])
	currOff += 4
	i.Gid = binary.LittleEndian.Uint32(bytes[currOff:])
	currOff += 4
	// symlink dest encoded as uint16 + string bytes
	lenSymlink := int(binary.LittleEndian.Uint16(bytes[currOff:]))
	currOff += 2
	i.Symlinkdest = string(bytes[currOff : currOff+lenSymlink])
	currOff += lenSymlink

	// blocks encoded as uint32 numBlocks + each block in a row
	numBlocks := binary.LittleEndian.Uint32(bytes[currOff:])
	currOff += 4
	i.Blocks = make([]Block, numBlocks, numBlocks)
	for j := uint32(0); j < numBlocks; j++ {
		currOff += i.Blocks[j].FromBytes(bytes[currOff:])
	}
	// now we do dentries -- N pairs of string and dentry
	numDentries := binary.LittleEndian.Uint32(bytes[currOff:])
	currOff += 4
	i.Children = make(map[string]Dentry)
	for j := uint32(0); j < numDentries; j++ {
		nameLen := int(binary.LittleEndian.Uint16(bytes[currOff:]))
		currOff += 2
		name := string(bytes[currOff : currOff+nameLen])
		currOff += nameLen
		dentryIno := binary.LittleEndian.Uint64(bytes[currOff:])
		currOff += 8
		dentryCtime := int64(binary.LittleEndian.Uint64(bytes[currOff:]))
		currOff += 8
		i.Children[name] = Dentry{dentryIno, dentryCtime}
	}
	return currOff
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

func (i *Inode) FullMode() uint32 {
	switch {
	case FTYPE_DIR == i.Ftype:
		return syscall.S_IFDIR | i.Mode
	case FTYPE_REG == i.Ftype:
		return syscall.S_IFREG | i.Mode
	case FTYPE_LNK == i.Ftype:
		return syscall.S_IFLNK | i.Mode
	}
	return syscall.S_IFREG | i.Mode
}

func (i *Inode) Equals(other *Inode) bool {
	if other == nil {
		return false
	}
	eq := (i.Inodeid == other.Inodeid && i.Generation == other.Generation &&
		i.Ftype == other.Ftype && i.Mode == other.Mode && i.Ctime == other.Ctime &&
		i.Mtime == other.Mtime && i.Nlink == other.Nlink && i.Uid == other.Uid && i.Gid == other.Gid && i.Symlinkdest == other.Symlinkdest)
	if !eq {
		return false
	}
	if len(i.Blocks) != len(other.Blocks) {
		return false
	}
	for idx, b := range i.Blocks {
		if b.Id != other.Blocks[idx].Id {
			return false
		}
	}
	for name, dentry := range i.Children {
		otherDentry, ok := other.Children[name]
		if !ok || dentry.Inodeid != otherDentry.Inodeid {
			return false
		}
	}
	return true
}

type Dentry struct {
	Inodeid     uint64
	CreatedTime int64 // time this link was created.  used to return consistent ordering in ReadDir.
}

func (i *Inode) IsDir() bool {
	return i.Ftype == FTYPE_DIR
}

type Block struct {
	Id       uint64   // globally unique block id
	Version  uint64   // revision number
	Inodeid  uint64   // which inode we belong to
	StartPos uint64   // alignment of this bytes first block in the file
	EndPos   uint64   // alignment of this bytes last block in the file
	Volumes  []uint32 // IDs for the volumes we're replicated over -- 0 is an invalid volume
}

func (b *Block) BinSize() int {
	// 40 for 5 longs
	size := 40
	// num volumes encoded as a single byte lol
	size++
	// now 4 for each int
	size += (4 * len(b.Volumes))
	return size
}

//func (b *Block) GobEncode() ([]byte,error) {
//	size := b.BinSize()
//	bytes := make([]byte,size,size)
//	b.ToBytes(bytes)
//	return bytes,nil
//}

// writes to the specified byte slice -- note that we don't bounds check here so make sure you ensure the correct size using BinSize()
// returns num bytes written
func (b *Block) ToBytes(bytes []byte) int {
	currOff := 0
	// write uint64s
	binary.LittleEndian.PutUint64(bytes[currOff:], b.Id)
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:], b.Version)
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:], b.Inodeid)
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:], b.StartPos)
	currOff += 8
	binary.LittleEndian.PutUint64(bytes[currOff:], b.EndPos)
	currOff += 8
	// write num volumes and then volumes
	bytes[currOff] = byte(len(b.Volumes))
	currOff++
	for _, v := range b.Volumes {
		binary.LittleEndian.PutUint32(bytes[currOff:], v)
		currOff += 4
	}
	return currOff
}

//func (b *Block) GobDecode(bytes []byte) {
//	b.FromBytes(bytes)
//	return
//}

// returns num bytes read
func (b *Block) FromBytes(bytes []byte) int {
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
	b.Volumes = make([]uint32, numVolumes, numVolumes)
	for i := byte(0); i < numVolumes; i++ {
		b.Volumes[i] = binary.LittleEndian.Uint32(bytes[currOff:])
		currOff += 4
	}
	return currOff
}

func (b *Block) Length() uint64 {
	return b.EndPos - b.StartPos + 1
}
