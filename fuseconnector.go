package maggiefs

import (
  "syscall"
  "sync/atomic"
  "github.com/hanwen/go-fuse/raw"
  "github.com/hanwen/go-fuse/fuse"
)

type mfile struct {
  r Reader
  w Writer
  writeable bool
  readable bool
  fh uint64
  appnd bool // whether we're in append mode
}

func (m *mfile) Close() (err error) {
  var ret error = nil
  err := r.Close()
  if (err != nil) { ret = err }
  err = w.Close()
  if (err != nil) { ret = err }
  return ret
}

type MaggieFuse struct {
  fs *MaggieFs
  openFiles map[uint64] *mfile
  fhCounter uint64
}

// FUSE implementation
func (m *MaggieFuse) Init(init *fuse.RawFsInit) {
}

func (m *MaggieFuse) String() string {
	return "MAGGIEFS"
}

func (m *MaggieFuse) StatFs(out *fuse.StatfsOut, h *raw.InHeader) fuse.Status {
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




func (m *MaggieFuse) Lookup(out *raw.EntryOut, h *raw.InHeader, name string) (code fuse.Status) {
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
  out.Blocks = numBlocks(i.Length, PAGESIZE)
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
  out.Blksize = PAGESIZE
  //out.padding = uint32(0) // dunno

	return fuse.OK
}

func (m *MaggieFuse) Forget(nodeID, nlookup uint64) {
  // noop
}

func (m *MaggieFuse) GetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.GetAttrIn) (code fuse.Status) {
  i,err := fs.names.GetInode(header.NodeId)
  if err != nil {
    return fuse.EROFS
  }
  out.Ino = i.Inodeid
  out.Size = i.Length
  out.Blocks = numBlocks(i.Length, PAGESIZE)
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
  out.Blksize = PAGESIZE
	return fuse.OK
}

func (m *MaggieFuse) Open(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
  // open flags
  readable,writable := parseWRFlags(input.Flags)
  // TODO handle other open flags here
  appnd := (input.Flags & syscall.O_APPEND != 0)

  // get inode
  inode,err := m.names.GetInode(header.NodeId)
  if err != nil {
    return fuse.EROFS
  }


  // allocate new filehandle
  fh := atomic.AddUint64(&m.fhCounter,uint64(1))
  r := Reader{}
  w := Writer{}
  if (readable) {
    r,err = NewReader(inode,m.datas)
    if (err != nil) { return fuse.EROFS }
  }
  if (writable) {
    w,err = NewWriter(inode,m.datas)
    if (err != nil) { return fuse.EROFS }
  }

  file := mfile {
    r,
    w,
    writable,
    fh,
    appnd} //append mode
  m.openFiles[fh] = &file
  // output
  out.fh = fh
  out.OpenFlags = fuse.FOPEN_KEEP_CACHE
  // return int val
	return fuse.OK
}


// returns whether readable, writable
func parseWRFlags(flags uint32) (bool, bool) {
  switch {
      case flags & syscall.O_RDWR != 0:
        return true,true
      case flags & syscall.O_RDONLY != 0:
        return true,false
      case flags & syscall.O_WRONLY != 0:
        return false,true

  }
}

func (m *MaggieFuse) SetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.SetAttrIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (m *MaggieFuse) Readlink(header *raw.InHeader) (out []byte, code fuse.Status) {
  // read string destination path for a symlink
	return nil, fuse.ENOSYS
}

func (m *MaggieFuse) Mknod(out *raw.EntryOut, header *raw.InHeader, input *raw.MknodIn, name string) (code fuse.Status) {
  // set up inode
  parent,err := m.names.GetInode(header.NodeId)
  if err != nil {
    return fuse.EROFS
  }
  newNode := Inode{

  }

  // save
	return fuse.ENOSYS

}

func (m *MaggieFuse) Mkdir(out *raw.EntryOut, header *raw.InHeader, input *raw.MkdirIn, name string) (code fuse.Status) {
  // set up inode
  // save
  // set up PathEntry type DIR pointing to inode
  // save
	return fuse.ENOSYS
}

func (m *MaggieFuse) Unlink(header *raw.InHeader, name string) (code fuse.Status) {
  // delete path entry pointing to inode
	return fuse.ENOSYS
}

func (m *MaggieFuse) Rmdir(header *raw.InHeader, name string) (code fuse.Status) {
  // delete path entry pointing to inode, then inode
	return fuse.ENOSYS
}

func (m *MaggieFuse) Symlink(out *raw.EntryOut, header *raw.InHeader, pointedTo string, linkName string) (code fuse.Status) {
  // new path entry
	return fuse.ENOSYS
}

func (m *MaggieFuse) Rename(header *raw.InHeader, input *raw.RenameIn, oldName string, newName string) (code fuse.Status) {
  // alter pathentry object, atomic operation
	return fuse.ENOSYS
}

func (m *MaggieFuse) Link(out *raw.EntryOut, header *raw.InHeader, input *raw.LinkIn, name string) (code fuse.Status) {
  // new pathentry object pointing to node, update refcount on node
	return fuse.ENOSYS
}

func (m *MaggieFuse) GetXAttrSize(header *raw.InHeader, attr string) (size int, code fuse.Status) {
  // punt
	return 0, fuse.ENOSYS
}

func (m *MaggieFuse) GetXAttrData(header *raw.InHeader, attr string) (data []byte, code fuse.Status) {
  // punt
	return nil, fuse.ENOSYS
}

func (m *MaggieFuse) SetXAttr(header *raw.InHeader, input *raw.SetXAttrIn, attr string, data []byte) fuse.Status {
  // punt
	return fuse.ENOSYS
}

func (m *MaggieFuse) ListXAttr(header *raw.InHeader) (data []byte, code fuse.Status) {
  //punt
	return nil, fuse.ENOSYS
}

func (m *MaggieFuse) RemoveXAttr(header *raw.InHeader, attr string) fuse.Status {
  // punt
	return fuse.ENOSYS
}

func (m *MaggieFuse) Access(header *raw.InHeader, input *raw.AccessIn) (code fuse.Status) {
  // check perms, always return ok
	return fuse.ENOSYS
}

func (m *MaggieFuse) Create(out *raw.CreateOut, header *raw.InHeader, input *raw.CreateIn, name string) (code fuse.Status) {
  // call mknod and then open
	return fuse.ENOSYS
}

func (m *MaggieFuse) OpenDir(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
  // fetch inode, store in map fd-> dirobject 
	return fuse.ENOSYS
}

func (m *MaggieFuse) Read(header *raw.InHeader, input *raw.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
  // read from map fd -> file
	return &fuse.ReadResultData{}, fuse.ENOSYS
}


func (m *MaggieFuse) Release(header *raw.InHeader, input *raw.ReleaseIn) {
  // release, wtf?
}

func (m *MaggieFuse) Write(header *raw.InHeader, input *raw.WriteIn, data []byte) (written uint32, code fuse.Status) {
  // write using map fd->file
	return 0, fuse.ENOSYS
}

func (m *MaggieFuse) Flush(header *raw.InHeader, input *raw.FlushIn) fuse.Status {
  // delegate to fsync
	return fuse.OK
}

func (m *MaggieFuse) Fsync(header *raw.InHeader, input *raw.FsyncIn) (code fuse.Status) {
  // sync using map fd->file
	return fuse.ENOSYS
}

func (m *MaggieFuse) ReadDir(l *fuse.DirEntryList, header *raw.InHeader, input *raw.ReadIn) fuse.Status {
  // read from map fd-> dirobject
	return fuse.ENOSYS
}

func (m *MaggieFuse) ReleaseDir(header *raw.InHeader, input *raw.ReleaseIn) {
  // drop from map fd->dirobject??
}

func (m *MaggieFuse) FsyncDir(header *raw.InHeader, input *raw.FsyncIn) (code fuse.Status) {
  // unnecessary because we persist anyways, i think?
	return fuse.ENOSYS
}
