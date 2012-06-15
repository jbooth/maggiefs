package maggiefs

import (
  "syscall"
  "github.com/hanwen/go-fuse/raw"
  "github.com/hanwen/go-fuse/fuse"
)

const(
  BLKSIZE = uint32(4096)
)

type MaggieFs struct {
  names NameService

}

func (fs *MaggieFs) Init(init *fuse.RawFsInit) {
}

func (fs *MaggieFs) String() string {
	return "MAGGIEFS"
}

func (fs *MaggieFs) StatFs(out *fuse.StatfsOut, h *raw.InHeader) fuse.Status {
  stat,err := fs.names.StatFs()
  if (err != nil) {
    return fuse.EROFS
  }
  out.Blocks = stat.Blocks
  out.Bfree = stat.Bfree
  out.Bavail = stat.Bavail
  out.Files = stat.Files
  out.Ffree =stat.Ffree
  out.Bsize = uint32(stat.Bsize)
  out.NameLen = uint32(stat.Namelen)
  out.Frsize = uint32(stat.Frsize)
  //out.Padding = uint32(0)
  out.Spare = [6]uint32{0,0,0,0,0,0}
  return fuse.OK
}

// all files are 0777 yaaaaaaay
func mode(ftype int) uint32 {
  switch {
    case FTYPE_DIR == ftype:
      return syscall.S_IFDIR | 0777
    case FTYPE_REG == ftype:
      return syscall.S_IFREG | 0777
    case FTYPE_LNK == ftype:
      return syscall.S_IFLNK | 0777
  }
  return syscall.S_IFREG | 0777
}

func numBlocks(size uint64, blksize uint32) uint64 {
  bs := uint64(blksize)
  leftover := size % bs
  numB := size / bs
  if (leftover != 0) {
    numB++
  }
  return numB
}

func (fs *MaggieFs) Lookup(out *raw.EntryOut, h *raw.InHeader, name string) (code fuse.Status) {
  // Lookup PathEntry by name
  p,i,err := fs.names.GetPathInode(name)
  if err != nil {
    return fuse.EROFS
  }
  // fill out
  out.NodeId = i.Inodeid
  out.Generation = i.Generation
  out.EntryValid = uint64(0)
  out.AttrValid = uint64(0)
  out.EntryValidNsec = uint32(0)
  out.AttrValidNsec = uint32(0)
  //Inode
  out.Ino = i.Inodeid
  out.Size = i.Length
  out.Blocks = uint64(len(i.BlockLocations))
  out.Atime = uint64(0) // always 0 for atime
  out.Mtime = i.Mtime // Mtime is user modifiable and is the last time data changed
  out.Ctime = i.Ctime // Ctime is tracked by the FS and changes when attrs or data change
  out.Atimensec = uint32(0)
  out.Mtimensec = uint32(0)
  out.Ctimensec = uint32(0)
  out.Mode = mode(p.Ftype)
  out.Nlink = i.Nlink
  out.Uid = p.Uid
  out.Gid = p.Gid
  out.Rdev = uint32(0) // regular file, not block dvice
  out.Blksize = BLKSIZE
  //out.padding = uint32(0) // dunno

	return fuse.OK
}

func (fs *MaggieFs) Forget(nodeID, nlookup uint64) {
}

func (fs *MaggieFs) GetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.GetAttrIn) (code fuse.Status) {
  i,err := fs.names.GetInode(header.NodeId)
  if err != nil {
    return fuse.EROFS
  }
  out.Ino = i.Inodeid
  out.Size = i.Length
  out.Blocks = numBlocks(i.Length, BLKSIZE)
  out.Atime = uint64(0) // always 0 for atime
  out.Mtime = i.Mtime // Mtime is user modifiable and is the last time data changed
  out.Ctime = i.Ctime // Ctime is tracked by the FS and changes when attrs or data change
  out.Atimensec = uint32(0)
  out.Mtimensec = uint32(0)
  out.Ctimensec = uint32(0)
  out.Mode = mode(i.Ftype)
  out.Nlink = i.Nlink
  out.Uid = i.Uid
  out.Gid = i.Gid
  out.Rdev = uint32(0) // regular file, not block dvice
  out.Blksize = BLKSIZE
	return fuse.OK
}


func (fs *MaggieFs) Open(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
	return fuse.OK
}

func (fs *MaggieFs) SetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.SetAttrIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Readlink(header *raw.InHeader) (out []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (fs *MaggieFs) Mknod(out *raw.EntryOut, header *raw.InHeader, input *raw.MknodIn, name string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Mkdir(out *raw.EntryOut, header *raw.InHeader, input *raw.MkdirIn, name string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Unlink(header *raw.InHeader, name string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Rmdir(header *raw.InHeader, name string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Symlink(out *raw.EntryOut, header *raw.InHeader, pointedTo string, linkName string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Rename(header *raw.InHeader, input *raw.RenameIn, oldName string, newName string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Link(out *raw.EntryOut, header *raw.InHeader, input *raw.LinkIn, name string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) GetXAttrSize(header *raw.InHeader, attr string) (size int, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *MaggieFs) GetXAttrData(header *raw.InHeader, attr string) (data []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (fs *MaggieFs) SetXAttr(header *raw.InHeader, input *raw.SetXAttrIn, attr string, data []byte) fuse.Status {
	return fuse.ENOSYS
}

func (fs *MaggieFs) ListXAttr(header *raw.InHeader) (data []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (fs *MaggieFs) RemoveXAttr(header *raw.InHeader, attr string) fuse.Status {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Access(header *raw.InHeader, input *raw.AccessIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Create(out *raw.CreateOut, header *raw.InHeader, input *raw.CreateIn, name string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) OpenDir(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) Read(header *raw.InHeader, input *raw.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	return &fuse.ReadResultData{}, fuse.ENOSYS
}

func (fs *MaggieFs) Release(header *raw.InHeader, input *raw.ReleaseIn) {
}

func (fs *MaggieFs) Write(header *raw.InHeader, input *raw.WriteIn, data []byte) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *MaggieFs) Flush(header *raw.InHeader, input *raw.FlushIn) fuse.Status {
	return fuse.OK
}

func (fs *MaggieFs) Fsync(header *raw.InHeader, input *raw.FsyncIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *MaggieFs) ReadDir(l *fuse.DirEntryList, header *raw.InHeader, input *raw.ReadIn) fuse.Status {
	return fuse.ENOSYS
}

func (fs *MaggieFs) ReleaseDir(header *raw.InHeader, input *raw.ReleaseIn) {
}

func (fs *MaggieFs) FsyncDir(header *raw.InHeader, input *raw.FsyncIn) (code fuse.Status) {
	return fuse.ENOSYS
}
