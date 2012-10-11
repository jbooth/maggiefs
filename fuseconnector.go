package maggiefs

import (
  "os"
  "time"
  "syscall"
  "sync/atomic"
  "sync"
  "github.com/hanwen/go-fuse/raw"
  "github.com/hanwen/go-fuse/fuse"
  "log"
  "bytes"
)

type MaggieFuse struct {
  names NameService
  datas DataService
  openFiles openFileMap // maps FD numbers to open files
  fdCounter uint64 // used to get unique FD numbers
  log *log.Logger
}

func NewMaggieFuse(names NameService, datas DataService) *MaggieFuse {
  return &MaggieFuse{
    names, 
    datas, 
    openFileMap{10, make(map[uint64]openFileMapSlice)},
    uint64(0), 
    log.New(os.Stderr, "maggie-fuse", 0),
  }
}

type OpenFile struct {
  r *Reader
  w *Writer
  lease Lease
  writelock WriteLock
}

func (f OpenFile) Close() error {
  var err error = nil
  if (f.r != nil) { err = f.r.Close() }
  if (f.w != nil) { err = f.w.Close() }
  err = f.lease.Release()
  if (f.writelock != nil) { err = f.writelock.Unlock() }
  return err
}

// ghetto concurrent hashmap
type openFileMap struct {
  numBuckets int
  mapmap map[uint64] openFileMapSlice
}

type openFileMapSlice struct {
  files map[uint64] OpenFile
  lock sync.RWMutex
}

func newOpenFileMap(numBuckets int) openFileMap {
  ret := openFileMap { numBuckets, make(map[uint64] openFileMapSlice) }
  for i := 0 ; i < numBuckets ; i++ {
    ret.mapmap[uint64(i)] = openFileMapSlice{make(map[uint64] OpenFile), sync.RWMutex{}}
  }
  return ret
}

func (m openFileMap) put(k uint64, v OpenFile) {
  slice := m.mapmap[k % uint64(m.numBuckets)]
  slice.lock.Lock()
  defer slice.lock.Unlock()
  slice.files[k] = v
}

func (m openFileMap) get(k uint64) OpenFile {
  slice := m.mapmap[k % uint64(m.numBuckets)]
  slice.lock.RLock()
  defer slice.lock.RUnlock()
  return slice.files[k]
}


// hint to namenode for incremental GC

// FUSE implementation
func (m *MaggieFuse) Init(init *fuse.RawFsInit) {
}

func (m *MaggieFuse) String() string {
	return "MAGGIEFS"
}

func (m *MaggieFuse) StatFs(out *fuse.StatfsOut, h *raw.InHeader) fuse.Status {
  stat,err := m.names.StatFs()
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
  // fetch parent
  parent,err := m.names.GetInode(h.NodeId)
  childId := parent.Children[name]
  // Lookup PathEntry by name
  child,err := m.names.GetInode(childId)
  if err != nil { return fuse.EIO }
  //if (child == nil) { return fuse.ENOENT }
  fillEntryOut(out,child)
	return fuse.OK
}

func fillEntryOut(out *raw.EntryOut, i *Inode) {
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
  out.Mtime = uint64(i.Mtime) // Mtime is user modifiable and is the last time data changed
  out.Ctime = uint64(i.Ctime) // Ctime is tracked by the FS and changes when attrs or data change
  out.Atimensec = uint32(0)
  out.Mtimensec = uint32(0)
  out.Ctimensec = uint32(0)
  out.Mode = mode(i.Ftype)
  out.Nlink = i.Nlink
  out.Uid = i.Uid
  out.Gid = i.Gid
  out.Rdev = uint32(0) // regular file, not block dvice
  out.Blksize = PAGESIZE
}

func (m *MaggieFuse) Forget(nodeID, nlookup uint64) {
  // noop
}

func (m *MaggieFuse) GetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.GetAttrIn) (code fuse.Status) {
  i,err := m.names.GetInode(header.NodeId)
  if err != nil {
    return fuse.EROFS
  }
  // raw.Attr
  out.Ino = i.Inodeid
  out.Size = i.Length
  out.Blocks = numBlocks(i.Length, PAGESIZE)
  out.Atime = uint64(0) // always 0 for atime
  out.Mtime = uint64(i.Mtime) // Mtime is user modifiable and is the last time data changed
  out.Ctime = uint64(i.Ctime) // Ctime is tracked by the FS and changes when attrs or data change
  out.Atimensec = uint32(0)
  out.Mtimensec = uint32(0)
  out.Ctimensec = uint32(0)
  out.Mode = i.Mode
  out.Nlink = i.Nlink
  out.Uid = i.Uid
  out.Gid = i.Gid
  out.Rdev = uint32(0) // regular file, not block dvice
  out.Blksize = PAGESIZE
  // raw.AttrOut
  out.AttrValid = uint64(0)
  out.AttrValidNsec = uint32(0)
	return fuse.OK
}

func (m *MaggieFuse) Open(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
  // TODO handle open flags

  // if read, readable = true

  // if write, then
    // if file length = 0, open fine
    // if file length > 0, we must be either TRUNC or APPEND
  // open flags
  readable,writable := parseWRFlags(input.Flags)
  //appnd := (input.Flags & syscall.O_APPEND != 0)

  // get inode
  inode,err := m.names.GetInode(header.NodeId)
  if err != nil {
    return fuse.EROFS
  }



  // allocate new filehandle
  fh := atomic.AddUint64(&m.fdCounter,uint64(1))
  f := OpenFile{nil,nil,nil,nil}
  f.lease,err = m.names.Lease(inode.Inodeid)
  if (err != nil) { 
    return fuse.EROFS
  }
  if (readable) {
    f.r,err = NewReader(inode.Inodeid,m.names,m.datas)
    if (err != nil) { return fuse.EROFS }
  }
  if (writable) {
    f.w,err = NewWriter(inode.Inodeid,m.names,m.datas)
    if (err != nil) { return fuse.EROFS }
    f.writelock,err = m.names.WriteLock(inode.Inodeid)
    if (err != nil) { return fuse.EROFS }
  }

  // output
  out.Fh = fh
  out.OpenFlags = raw.FOPEN_KEEP_CACHE
  m.openFiles.put(fh,f)
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
  // shouldn't happen
  return false,false
}

func (m *MaggieFuse) SetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.SetAttrIn) (code fuse.Status) {
  if (input.Valid & (raw.FATTR_MODE | raw.FATTR_UID | raw.FATTR_GID | raw.FATTR_MTIME | raw.FATTR_MTIME_NOW) == 0) {
    // if none of the items we care about were modified, skip it
    return fuse.OK
  }
  _,err := m.names.Mutate(header.NodeId, func (inode *Inode) error {
    // input.Valid is a bitmask for which fields are modified, so we check for each
    if (input.Valid & raw.FATTR_MODE != 0) {
      inode.Mode = input.Mode
    }
    if (input.Valid & raw.FATTR_UID != 0) {
      inode.Uid = input.Uid
    }
    if (input.Valid & raw.FATTR_GID != 0) {
      inode.Gid = input.Gid
    }
    // mark mtime updated
    inode.Mtime = time.Now().Unix()
    inode.Ctime = inode.Mtime
    return nil
  })
  if (err != nil) { return fuse.EROFS }

  return fuse.OK
}

func (m *MaggieFuse) Readlink(header *raw.InHeader) (out []byte, code fuse.Status) {
  // read string destination path for a symlink
  symlink,err := m.names.GetInode(header.NodeId)
  if (err != nil) { return nil,fuse.EROFS }
	return []byte(symlink.Symlinkdest), fuse.OK
}

func (m *MaggieFuse) Mknod(out *raw.EntryOut, header *raw.InHeader, input *raw.MknodIn, name string) (code fuse.Status) {
  // write lock on parent so 2 processes can't add the same child simultaneously
  wlock,err := m.names.WriteLock(header.NodeId)
  defer wlock.Unlock()
  if err != nil {
    return fuse.EROFS
  }
  //get parent
  parent,err := m.names.GetInode(header.NodeId)
  _,alreadyHasChild := parent.Children[name]
  if (alreadyHasChild) {
    return fuse.EINVAL
  }
  currTime := time.Now().Unix()
  i := Inode{
    0, // id 0 to start
    0, // gen 0
    FTYPE_REG,
    0,
    0x775,
    currTime,
    currTime,
    0,
    header.Uid,
    header.Gid,
    "",
    make([]Block,0,100),
    map[string] uint64 {},
    map[string] []byte {},
    }

  // save new node
  id,err := m.names.AddInode(i)
  if err != nil {
    return fuse.EROFS
  }
  i.Inodeid = id

  // link parent
  _,err = m.names.Mutate(parent.Inodeid, func (inode *Inode) error {
    inode.Children[name] = i.Inodeid
    return nil
  })
  if (err != nil) {
    return fuse.EROFS
  }
  // output
  fillEntryOut(out,&i)
	return fuse.OK

}

func (m *MaggieFuse) Mkdir(out *raw.EntryOut, header *raw.InHeader, input *raw.MkdirIn, name string) (code fuse.Status) {
  wlock, err := m.names.WriteLock(header.NodeId)
  defer wlock.Unlock()
  if (err != nil) {
    return fuse.EROFS
  }
  // lookup parent
  parent,err := m.names.GetInode(header.NodeId)
  if (err != nil) {
    return fuse.EROFS
  }
  if (! parent.IsDir()) { return fuse.Status(syscall.ENOTDIR) }
  // check if we already have dir of this name
  _,alreadyHasChild := parent.Children[name]
  if (alreadyHasChild) {
    return fuse.Status(syscall.EEXIST)
  }

  // make new child
  currTime := time.Now().Unix()
  i := Inode{
    0, // id 0 to start, we get id when inserting
    0,
    FTYPE_DIR,
    0,
    0x777,
    currTime,
    currTime,
    0,
    header.Uid,
    header.Gid,
    "",
    make([]Block,0,0),
    map[string] uint64{},
    map[string] []byte {},
    }

  // save
  id,err := m.names.AddInode(i)
  if err != nil {
    return fuse.EROFS
  }
  i.Inodeid = id
  // link parent
  _,err = m.names.Mutate(parent.Inodeid, func (inode *Inode) error {
    inode.Children[name] = i.Inodeid
    return nil
  })
  if (err != nil) { 
    return fuse.EROFS
  }
  // send entry back to child
  fillEntryOut(out,&i)
  return fuse.OK
}

func (m *MaggieFuse) Unlink(header *raw.InHeader, name string) (code fuse.Status) {
  // pull parent
  parent,err := m.names.GetInode(header.NodeId)
  if (err != nil) { return fuse.EROFS }
  // check if name doesn't exist
  _,hasChild := parent.Children[name]
  if (!hasChild) {
    return fuse.EINVAL
  }
  // look up node for name
  child,err := m.names.GetInode(parent.Children[name])
  if (err != nil) { return fuse.EROFS }

  // if child is dir, err
  if (child.IsDir()) { return fuse.Status(syscall.EISDIR) }

  // save parent without link 
  _,err = m.names.Mutate(parent.Inodeid,func (node *Inode) error {
    delete(node.Children,name)
    return nil
  })
  if (err != nil) { return fuse.EROFS }
  // decrement refcount
  _,err = m.names.Mutate(child.Inodeid,func (node *Inode) error {
    node.Nlink--
    return nil
  })
  if (err != nil) { return fuse.EROFS }
	return fuse.OK
}

func (m *MaggieFuse) Rmdir(header *raw.InHeader, name string) (code fuse.Status) {
  // pull parent
  parent,err := m.names.GetInode(header.NodeId)
  if (err != nil) { return fuse.EROFS }
  // check if name doesn't exist
  _,hasChild := parent.Children[name]
  if (!hasChild) {
    return fuse.EINVAL
  }
  // look up node for name
  child,err := m.names.GetInode(parent.Children[name])
  if (err != nil) { return fuse.EROFS }

  // if child is not dir, err
  if (! child.IsDir()) { return fuse.Status(syscall.ENOTDIR) }
  if (len(parent.Children) != 0) { return fuse.Status(syscall.ENOTEMPTY) }

  // save parent without link 
  _,err = m.names.Mutate(parent.Inodeid,func (node *Inode) error {
    delete(node.Children,name)
    return nil
  })
  if (err != nil) { return fuse.EROFS }
  // decrement refcount
  _,err = m.names.Mutate(child.Inodeid,func (node *Inode) error {
    node.Nlink--
    return nil
  })
  if (err != nil) { return fuse.EROFS }
	return fuse.OK
}

func (m *MaggieFuse) Symlink(out *raw.EntryOut, header *raw.InHeader, pointedTo string, linkName string) (code fuse.Status) {
  // new inode type symlink
  currTime := time.Now().Unix()
  i := Inode{
    0, // id 0 to start, we get id when inserting
    0,
    FTYPE_LNK,
    0,
    0x777,
    currTime,
    currTime,
    0,
    header.Uid,
    header.Gid,
    pointedTo,
    make([]Block,0,0),
    map[string] uint64{},
    map[string] []byte {},
  }
  // save
  id,err := m.names.AddInode(i)
  if err != nil {
    return fuse.EROFS
  }
  i.Inodeid = id
  // link parent
  _,err = m.names.Mutate(header.NodeId, func (inode *Inode) error {
    inode.Children[linkName] = i.Inodeid
    return nil
  })
  if (err != nil) { 
    return fuse.EROFS
  }
  // send entry back to child
  fillEntryOut(out,&i)
  return fuse.OK
}

func (m *MaggieFuse) Rename(header *raw.InHeader, input *raw.RenameIn, oldName string, newName string) (code fuse.Status) {
  // gonna do an unlink and a link here while skipping the refcount steps in between
  // pull old parent
  oldParent,err := m.names.GetInode(header.NodeId)
  if (err != nil) { return fuse.EROFS }
  // check if name doesn't exist
  _,hasChild := oldParent.Children[oldName]
  if (!hasChild) {
    return fuse.EINVAL
  }
  // look up nodeid for name
  childNodeId := oldParent.Children[oldName]
  // save parent without link 
  _,err = m.names.Mutate(oldParent.Inodeid,func (node *Inode) error {
    delete(node.Children,oldName)
    return nil
  })
  if (err != nil) { return fuse.EROFS } 

  // save new parent with link
  _,err = m.names.Mutate(input.Newdir, func(node *Inode) error {
    node.Children[newName] = childNodeId
    return nil
  })
  if (err != nil) { return fuse.EROFS }
  // ref counts should all stay the same

	return fuse.OK
}

func (m *MaggieFuse) Link(out *raw.EntryOut, header *raw.InHeader, input *raw.LinkIn, name string) (code fuse.Status) {
  // new parent is header.NodeId
  // existing node is input.Oldnodeid
  
  // add link to new parent
  m.names.Mutate(header.NodeId, func(node *Inode) error {
    node.Children[name] = input.Oldnodeid
    return nil
  })

  // increment refcount on child
  m.names.Mutate(input.Oldnodeid, func(node *Inode) error {
    node.Nlink++
    return nil
  })

	return fuse.OK
}

func (m *MaggieFuse) GetXAttrSize(header *raw.InHeader, attr string) (size int, code fuse.Status) {
  node,err := m.names.GetInode(header.NodeId)
  if (err != nil) { return 0,fuse.EROFS }

	return len(node.Xattr), fuse.OK
}

func (m *MaggieFuse) GetXAttrData(header *raw.InHeader, attr string) (data []byte, code fuse.Status) {
  node,err := m.names.GetInode(header.NodeId)
  if (err != nil) { return nil,fuse.EROFS }
  
  // punt
	return node.Xattr[attr], fuse.OK
}

func (m *MaggieFuse) SetXAttr(header *raw.InHeader, input *raw.SetXAttrIn, attr string, data []byte) fuse.Status {
  _,err := m.names.Mutate(header.NodeId, func(node *Inode) error {
    node.Xattr[attr] = data
    return nil
  })
  if (err != nil) { return fuse.EROFS }
  // punt
	return fuse.OK
}

func (m *MaggieFuse) ListXAttr(header *raw.InHeader) (data []byte, code fuse.Status) {
  node,err := m.names.GetInode(header.NodeId)
  if (err != nil) { return nil,fuse.EROFS }
  b := bytes.NewBuffer([]byte{})
  for k,_ := range node.Xattr {
    b.Write([]byte(k))
    b.WriteByte(0)
  }
	return b.Bytes(), fuse.ENOSYS
}

func (m *MaggieFuse) RemoveXAttr(header *raw.InHeader, attr string) fuse.Status {
  _,err := m.names.Mutate(header.NodeId, func(node *Inode) error {
    delete(node.Xattr,attr)
    return nil
  })
  if (err != nil) { return fuse.EROFS }
	return fuse.OK
}

func (m *MaggieFuse) Access(header *raw.InHeader, input *raw.AccessIn) (code fuse.Status) {
  // check perms, always return ok
  // we're not doing perms yet handle later
	return fuse.OK
}

func (m *MaggieFuse) Create(out *raw.CreateOut, header *raw.InHeader, input *raw.CreateIn, name string) (code fuse.Status) {
  // call mknod and then open
  mknodin := raw.MknodIn { input.Mode, uint32(0), input.Umask, input.Padding }
  stat := m.Mknod(&out.EntryOut, header,&mknodin, name)
  if (! stat.Ok()) { return stat }
  openin := raw.OpenIn{input.Flags, uint32(0)}
  stat = m.Open(&out.OpenOut, header, &openin)
  if (! stat.Ok()) { return stat }
	return fuse.OK
}

func (m *MaggieFuse) OpenDir(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
  // noop, we do stateless dirs
	return fuse.OK
}

func (m *MaggieFuse) Read(header *raw.InHeader, input *raw.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
  reader := m.openFiles.get(input.Fh).r
  nRead := uint32(0)
  for ; nRead < input.Size ; {
    n,err := reader.ReadAt(buf, input.Offset + uint64(nRead), input.Size - nRead)
    nRead += n
    if (err != nil) { return &fuse.ReadResultData{buf}, fuse.EROFS }
  }
  // read from map fd -> file
	return &fuse.ReadResultData{buf}, fuse.OK
}


func (m *MaggieFuse) Release(header *raw.InHeader, input *raw.ReleaseIn) {
  f := m.openFiles.get(input.Fh)
  err := f.Close()
  if (err != nil) {
    m.log.Print("error closing file")
  }
}

func (m *MaggieFuse) Write(header *raw.InHeader, input *raw.WriteIn, data []byte) (written uint32, code fuse.Status) {
  writer := m.openFiles.get(input.Fh).w
  written = uint32(0)
  for ; written < input.Size ; {
    n,err := writer.WriteAt(data, input.Offset + uint64(written), input.Size - written)
    written += n
    if (err != nil) { return written, fuse.EROFS }
  }
	return written,fuse.OK
}

func (m *MaggieFuse) Flush(header *raw.InHeader, input *raw.FlushIn) fuse.Status {
  writer := m.openFiles.get(input.Fh).w
  err := writer.Fsync()
  if (err != nil) { return fuse.EROFS }
	return fuse.OK
}

func (m *MaggieFuse) Fsync(header *raw.InHeader, input *raw.FsyncIn) (code fuse.Status) {
  writer := m.openFiles.get(input.Fh).w
  err := writer.Fsync()
  if (err != nil) { return fuse.EROFS }
	return fuse.OK
}

func (m *MaggieFuse) ReadDir(l *fuse.DirEntryList, header *raw.InHeader, input *raw.ReadIn) fuse.Status {
  // read from map fd-> dirobject
  dir,err := m.names.GetInode(header.NodeId)
  if (err != nil) {
    return fuse.EROFS
  }
  for name,id := range dir.Children {
    l.Add(name,id,uint32(0777))
  }
	return fuse.OK
}

func (m *MaggieFuse) ReleaseDir(header *raw.InHeader, input *raw.ReleaseIn) {
  // noop, we do stateless dirs
}

func (m *MaggieFuse) FsyncDir(header *raw.InHeader, input *raw.FsyncIn) (code fuse.Status) {
  // unnecessary because we persist on all dir ops anyways
	return fuse.OK
}
