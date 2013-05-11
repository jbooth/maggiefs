package client

import (
	"bytes"
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/raw"
	"github.com/jbooth/maggiefs/maggiefs"
	"io"
	"log"
	"os"
	"sort"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	PAGESIZE    = maggiefs.PAGESIZE
	BLOCKLENGTH = maggiefs.BLOCKLENGTH
)

type MaggieFuse struct {
	leases         maggiefs.LeaseService
	names          maggiefs.NameService
	datas          maggiefs.DataService
	openFiles      openFileMap                                // maps FD numbers to open files
	fdCounter      uint64                                     // used to get unique FD numbers
	changeNotifier chan uint64                  // remote changes to inodes are notified through this chan
	inodeNotify    func(*raw.NotifyInvalInodeOut) fuse.Status // used to signal to OS that a remote inode changed
	log            *log.Logger
}

func NewMaggieFuse(leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService) (*MaggieFuse, error) {
	notifier := leases.GetNotifier()
	m := &MaggieFuse{
		leases,
		names,
		datas,
		newOpenFileMap(10),
		uint64(0),
		notifier,
		nil,
		log.New(os.Stderr, "maggie-fuse", 0),
	}
	// 
	go func() {
		notify := &raw.NotifyInvalInodeOut{}
		for inodeid := range m.changeNotifier {
			notify.Ino = inodeid
			stat := m.inodeNotify(notify)
			fmt.Printf("notified for inode %d, got value %+v", notify.Ino, stat)
		}
	}()
	return m, nil
}

func (m *MaggieFuse) mutate(inodeid uint64, mutator func(i *maggiefs.Inode) error) (*maggiefs.Inode,error) {
    wl, err := m.leases.WriteLease(inodeid)
    defer wl.Release()
    if err != nil {
      return nil,err
    }
    inode, err := m.names.GetInode(inodeid)
    if err != nil {
      return nil,err
    }
    err = mutator(inode)
    if err != nil { return nil,err }
    inode.Mtime = int64(time.Now().Unix())
    err = m.names.SetInode(inode)
    return inode,err
}
 
// FUSE implementation
func (m *MaggieFuse) Init(init *fuse.RawFsInit) {
	m.inodeNotify = init.InodeNotify
}

func (m *MaggieFuse) String() string {
	return "MAGGIEFS"
}

func (m *MaggieFuse) StatFs(out *fuse.StatfsOut, h *raw.InHeader) fuse.Status {
	stat, err := m.names.StatFs()
	if err != nil {
		return fuse.EROFS
	}
	// translate our number of bytes free to a number of 4k pages
	out.Bsize = uint32(4096)
	out.Frsize = uint32(4096)
	freeBlocks := stat.Free / 4096
	out.Bfree = freeBlocks
	out.Bavail = freeBlocks
	out.Blocks = stat.Size / 4096 // total size
	//out.Padding = uint32(0)
	out.Spare = [6]uint32{0, 0, 0, 0, 0, 0}
	return fuse.OK
}

// all files are 0777 yay
func mode(ftype uint32) uint32 {
	switch {
	case maggiefs.FTYPE_DIR == ftype:
		return syscall.S_IFDIR | 0777
	case maggiefs.FTYPE_REG == ftype:
		return syscall.S_IFREG | 0777
	case maggiefs.FTYPE_LNK == ftype:
		return syscall.S_IFLNK | 0777
	}
	return syscall.S_IFREG | 0777
}

func numBlocks(size uint64, blksize uint32) uint64 {
	bs := uint64(blksize)
	leftover := size % bs
	numB := size / bs
	if leftover != 0 {
		numB++
	}
	return numB
}

func (m *MaggieFuse) Lookup(out *raw.EntryOut, h *raw.InHeader, name string) (code fuse.Status) {
	// fetch parent
	parent, err := m.names.GetInode(h.NodeId)
	childEntry, exists := parent.Children[name]
	if !exists {
		return fuse.ENOENT
	}
	// Lookup PathEntry by name
	child, err := m.names.GetInode(childEntry.Inodeid)
	if err != nil {
		return fuse.EIO
	}
	//if (child == nil) { return fuse.ENOENT }
	fillEntryOut(out, child)
	return fuse.OK
}

func fillEntryOut(out *raw.EntryOut, i *maggiefs.Inode) {
	// fill out
	out.NodeId = i.Inodeid
  out.Generation = i.Generation
	out.EntryValid = uint64(0)
	out.AttrValid = uint64(0)
	out.EntryValidNsec = uint32(100)
	out.AttrValidNsec = uint32(100)
	//Inode
	out.Ino = i.Inodeid
	out.Size = i.Length
	out.Blocks = numBlocks(i.Length, maggiefs.PAGESIZE)
	out.Atime = uint64(0)       // always 0 for atime
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
	out.Blksize = maggiefs.PAGESIZE
}

func (m *MaggieFuse) Forget(nodeID, nlookup uint64) {
	// noop
}

func fillAttrOut(out *raw.AttrOut, i *maggiefs.Inode) {
	// raw.Attr
	out.Ino = i.Inodeid
	out.Size = i.Length
	out.Blocks = numBlocks(i.Length, maggiefs.PAGESIZE)
	out.Atime = uint64(0)       // always 0 for atime
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
	out.Blksize = maggiefs.PAGESIZE
	// raw.AttrOut
	out.AttrValid = uint64(0)
	out.AttrValidNsec = uint32(100)
}

func (m *MaggieFuse) GetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.GetAttrIn) (code fuse.Status) {
	i, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return fuse.EROFS
	}
	fillAttrOut(out, i)
	return fuse.OK
}

func (m *MaggieFuse) Open(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
	// TODO handle open flags

	// if read, readable = true

	// if write, then
	// if file length = 0, open fine
	// if file length > 0, we must be either TRUNC or APPEND
	// open flags
	readable, writable, truncate, appnd := parseWRFlags(input.Flags)

	fmt.Printf("opening inode %d with flag %b, readable %t writable %t truncate %t append %t \n", header.NodeId, input.Flags, readable, writable, truncate, appnd)

	// get inode
	inode, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return fuse.EROFS
	}

	if truncate {
		// clear file before writing
	}

	if appnd {
		// make sure file writer is in append mode
	}

	// allocate new filehandle
	fh := atomic.AddUint64(&m.fdCounter, uint64(1))
	f := openFile{nil, nil, nil, nil}
	f.lease, err = m.leases.ReadLease(inode.Inodeid)
	if err != nil {
		return fuse.EROFS
	}
	if readable {
		f.r, err = NewReader(inode.Inodeid, m.names, m.datas)
		if err != nil {
			return fuse.EROFS
		}
	}
	if writable {
		f.w, err = NewInodeWriter(inode.Inodeid, m.leases, m.names, m.datas)
		if err != nil {
			return fuse.EROFS
		}
		//    f.writelock,err = m.names.WriteLease(inode.Inodeid)
		//    if (err != nil) { return fuse.EROFS }
	}

	// output
	out.Fh = fh
	out.OpenFlags = raw.FOPEN_KEEP_CACHE
	fmt.Printf("putting openFile %+v in map under key %d\n", f, fh)
	m.openFiles.put(fh, &f)
	// return int val
	return fuse.OK
}

// returns whether readable, writable, truncate, append
func parseWRFlags(flags uint32) (bool, bool, bool, bool) {
	// default is read only
	readable, writable, truncate, appnd := true, false, false, false
	switch {
	case int(flags)&os.O_RDWR != 0:
		readable = true
		writable = true
	case int(flags)&os.O_WRONLY != 0:
		readable = false
		writable = true
	case int(flags)&os.O_TRUNC != 0:
		writable = true
		truncate = true
	case int(flags)&os.O_APPEND != 0:
		appnd = true
		writable = true

	}
	// not handling O_CREAT just yet
	return readable, writable, truncate, appnd
}

func (m *MaggieFuse) SetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.SetAttrIn) (code fuse.Status) {
	var f *openFile
	if input.Valid&raw.FATTR_FH != 0 {
		f = m.openFiles.get(input.Fh)
	}
	// if this is a truncate, handle the truncation separately from other mutations, it requires a special call
	if input.Valid&raw.FATTR_SIZE != 0 {
		if f == nil {
			return fuse.ENOENT
		}
		err := f.w.Truncate(input.Size)
		if err != nil {
			return fuse.EROFS
		}
	}

	// other mutations, if applicable
	if input.Valid&(raw.FATTR_MODE|raw.FATTR_UID|raw.FATTR_GID|raw.FATTR_MTIME|raw.FATTR_MTIME_NOW|raw.FATTR_MTIME|raw.FATTR_MTIME_NOW) != 0 {
    
		wl, err := m.leases.WriteLease(header.NodeId)
		defer wl.Release()
		if err != nil {
			return fuse.EROFS
		}
		inode, err := m.names.GetInode(header.NodeId)
		if err != nil {
			return fuse.EROFS
		}
		if input.Valid&raw.FATTR_MODE != 0 {
			// chmod
			inode.Mode = uint32(07777) & input.Mode
		}
		if input.Valid&(raw.FATTR_UID|raw.FATTR_GID) != 0 {
			// chown
			inode.Uid = input.Uid
			inode.Gid = input.Gid
		}
		if input.Valid&(raw.FATTR_MTIME|raw.FATTR_MTIME_NOW) != 0 {
			// MTIME, not supporting ATIME
			if input.Valid&raw.FATTR_MTIME_NOW != 0 {
				inode.Mtime = time.Now().UnixNano()
			} else {
				inode.Mtime = int64(input.Mtime*1e9) + int64(input.Mtimensec)
			}
		}
		// set on nameserver and on argument back out to fuse
		err = m.names.SetInode(inode)
		if err != nil {
			return fuse.EROFS
		}
		fillAttrOut(out, inode)
	}

	return fuse.OK
}

func (m *MaggieFuse) Readlink(header *raw.InHeader) (out []byte, code fuse.Status) {
	// read string destination path for a symlink
	symlink, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return nil, fuse.EROFS
	}
	return []byte(symlink.Symlinkdest), fuse.OK
}

func (m *MaggieFuse) Mknod(out *raw.EntryOut, header *raw.InHeader, input *raw.MknodIn, name string) (code fuse.Status) {

	//build node
	currTime := time.Now().Unix()
	i := maggiefs.Inode{
		0, // id 0 to start
		0, // gen 0
		maggiefs.FTYPE_REG,
		0,
		input.Mode & 07777,
		currTime,
		currTime,
		1,
		header.Uid,
		header.Gid,
		"",
		make([]maggiefs.Block, 0, 0),
		make(map[string]maggiefs.Dentry),
		make(map[string][]byte),
	}

	// save new node
	id, err := m.names.AddInode(&i)
	if err != nil {
		return fuse.EROFS
	}
	i.Inodeid = id

	// link parent
	err = m.names.Link(header.NodeId, i.Inodeid, name, false)
	if err != nil {
		if err == maggiefs.E_EXISTS {
			return fuse.Status(syscall.EEXIST)
		} else {
			return fuse.EROFS
		}
	}
	// output
	fillEntryOut(out, &i)
	return fuse.OK

}

func (m *MaggieFuse) Mkdir(out *raw.EntryOut, header *raw.InHeader, input *raw.MkdirIn, name string) (code fuse.Status) {

	// make new child
	currTime := time.Now().Unix()
	i := maggiefs.Inode{
		0, // id 0 to start, we get id when inserting
		0,
		maggiefs.FTYPE_DIR,
		0,
		07777 & input.Mode,
		currTime,
		currTime,
		0,
		header.Uid,
		header.Gid,
		"",
		make([]maggiefs.Block, 0, 0),
		make(map[string]maggiefs.Dentry),
		make(map[string][]byte),
	}

	// save
	id, err := m.names.AddInode(&i)
	if err != nil {
		return fuse.EROFS
	}
	i.Inodeid = id
	// link parent
	err = m.names.Link(header.NodeId, id, name, false)
	if err != nil {
		// garbage collector will clean up our 0 reference node
		if err == maggiefs.E_EXISTS {
			return fuse.Status(syscall.EEXIST)
		}
		if err == maggiefs.E_NOTDIR {
			return fuse.ENOTDIR
		}
		return fuse.EROFS
	}
	// send entry back to child
	fillEntryOut(out, &i)
	return fuse.OK
}

func (m *MaggieFuse) Unlink(header *raw.InHeader, name string) (code fuse.Status) {

	// finally unlink
	err := m.names.Unlink(header.NodeId, name)
	if err != nil {
		if err == maggiefs.E_NOENT {
			return fuse.ENOENT
		} else if err == maggiefs.E_ISDIR {
			return fuse.Status(syscall.EISDIR)
		}
		return fuse.EROFS
	}
	return fuse.OK
}

func (m *MaggieFuse) Rmdir(header *raw.InHeader, name string) (code fuse.Status) {
	// sanity checks on parent, child
	// pull parent
	parent, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return fuse.EROFS
	}
	// check if name doesn't exist
	childEntry, exists := parent.Children[name]
	if !exists {
		return fuse.ENOENT
	}

	// look up node for name
	child, err := m.names.GetInode(childEntry.Inodeid)
	if err != nil {
		return fuse.EROFS
	}

	// if child is not dir, err
	if !child.IsDir() {
		return fuse.Status(syscall.ENOTDIR)
	}
	fmt.Printf("removing directory %d with children %+v", child.Inodeid, child.Children)
	if len(child.Children) != 0 {
		return fuse.Status(syscall.ENOTEMPTY)
	}

	// actually unlink
	err = m.names.Unlink(parent.Inodeid, name)
	if err != nil {
		return fuse.EROFS
	}
	return fuse.OK
}

func (m *MaggieFuse) Symlink(out *raw.EntryOut, header *raw.InHeader, pointedTo string, linkName string) (code fuse.Status) {
	// new inode type symlink
	currTime := time.Now().Unix()
	i := maggiefs.Inode{
		0, // id 0 to start, we get id when inserting
		0,
		maggiefs.FTYPE_LNK,
		0,
		0777,
		currTime,
		currTime,
		0,
		header.Uid,
		header.Gid,
		pointedTo,
		make([]maggiefs.Block, 0, 0),
		make(map[string]maggiefs.Dentry),
		make(map[string][]byte),
	}
	// save
	id, err := m.names.AddInode(&i)
	if err != nil {
		return fuse.EROFS
	}
	i.Inodeid = id
	// link parent
	err = m.names.Link(header.NodeId, i.Inodeid, linkName, false)
	if err != nil {
		if err == maggiefs.E_EXISTS {
			return fuse.Status(syscall.EEXIST)
		} else if err == maggiefs.E_NOTDIR {
			return fuse.ENOTDIR
		}
		return fuse.EROFS
	}
	// send entry back to child
	fillEntryOut(out, &i)
	return fuse.OK
}

func (m *MaggieFuse) Rename(header *raw.InHeader, input *raw.RenameIn, oldName string, newName string) (code fuse.Status) {
	// look up old parent and get our inode
	oldParent, err := m.names.GetInode(header.NodeId)
	childDentry, exists := oldParent.Children[oldName]
	if !exists {
		return fuse.EINVAL
	}
	// link to new parent (force)
	err = m.names.Link(input.Newdir, childDentry.Inodeid, newName, true)
	if err != nil {
		if err == maggiefs.E_NOENT {
			return fuse.ENOENT
		}
		return fuse.EROFS
	}
	// unlink from old
	err = m.names.Unlink(oldParent.Inodeid, oldName)
	if err != nil {
		return fuse.EROFS
	}
	return fuse.OK
}

func (m *MaggieFuse) Link(out *raw.EntryOut, header *raw.InHeader, input *raw.LinkIn, name string) (code fuse.Status) {
	// new parent is header.NodeId
	// existing node is input.Oldnodeid

	// add link to new parent
	err := m.names.Link(header.NodeId, input.Oldnodeid, name, false)
	if err == maggiefs.E_EXISTS {
		return fuse.Status(syscall.EEXIST)
	} else if err != nil {
		return fuse.EROFS
	}

	return fuse.OK
}

func (m *MaggieFuse) GetXAttrSize(header *raw.InHeader, attr string) (size int, code fuse.Status) {
	node, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return 0, fuse.EROFS
	}

	return len(node.Xattr), fuse.OK
}

func (m *MaggieFuse) GetXAttrData(header *raw.InHeader, attr string) (data []byte, code fuse.Status) {
	node, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return nil, fuse.EROFS
	}

	// punt
	return node.Xattr[attr], fuse.OK
}

func (m *MaggieFuse) SetXAttr(header *raw.InHeader, input *raw.SetXAttrIn, attr string, data []byte) fuse.Status {
	_,err := m.mutate(header.NodeId, func(node *maggiefs.Inode) error {
		node.Xattr[attr] = data
		return nil
	})
	if err != nil {
		return fuse.EROFS
	}
	// punt
	return fuse.OK
}

func (m *MaggieFuse) ListXAttr(header *raw.InHeader) (data []byte, code fuse.Status) {
	node, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return nil, fuse.EROFS
	}
	b := bytes.NewBuffer([]byte{})
	for k, _ := range node.Xattr {
		b.Write([]byte(k))
		b.WriteByte(0)
	}
	return b.Bytes(), fuse.ENOSYS
}

func (m *MaggieFuse) RemoveXAttr(header *raw.InHeader, attr string) fuse.Status {
	_,err := m.mutate(header.NodeId, func(node *maggiefs.Inode) error {
		delete(node.Xattr, attr)
		return nil
	})
	if err != nil {
		return fuse.EROFS
	}
	return fuse.OK
}

func (m *MaggieFuse) Access(header *raw.InHeader, input *raw.AccessIn) (code fuse.Status) {
	// check perms, always return ok
	// we're not doing perms yet handle later
	return fuse.OK
}

func (m *MaggieFuse) Create(out *raw.CreateOut, header *raw.InHeader, input *raw.CreateIn, name string) (code fuse.Status) {
	// call mknod and then open
	mknodin := raw.MknodIn{input.Mode, uint32(0), input.Umask, input.Padding}
	stat := m.Mknod(&out.EntryOut, header, &mknodin, name)
	if !stat.Ok() {
		return stat
	}
	openin := raw.OpenIn{input.Flags, uint32(0)}
	// set header.NodeId to child, because that's what we actually open
	header.NodeId = out.NodeId
	stat = m.Open(&out.OpenOut, header, &openin)
	if !stat.Ok() {
		return stat
	}
	return fuse.OK
}

func (m *MaggieFuse) OpenDir(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
	// noop, we do stateless dirs
	return fuse.OK
}

func (m *MaggieFuse) Read(header *raw.InHeader, input *raw.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	reader := m.openFiles.get(input.Fh).r
	nRead := uint32(0)
	for nRead < input.Size {
		n, err := reader.ReadAt(buf, input.Offset+uint64(nRead), nRead, input.Size-nRead)
		nRead += n
		if err != nil {
			if err == io.EOF {
				fmt.Printf("returning data numbytes %d val %s\n", nRead, string(buf[0:int(nRead)]))
				return &fuse.ReadResultData{buf[0:int(nRead)]}, fuse.OK
			} else {
				fmt.Printf("Error reading %s\n", err.Error())
				return &fuse.ReadResultData{buf}, fuse.EROFS
			}
		}
	}
	// read from map fd -> file
	fmt.Printf("returning data %s\n", string(buf))
	return &fuse.ReadResultData{buf}, fuse.OK
}

func (m *MaggieFuse) Release(header *raw.InHeader, input *raw.ReleaseIn) {
	f := m.openFiles.get(input.Fh)
	err := f.Close()
	if err != nil {
		m.log.Print("error closing file")
	}
}

func (m *MaggieFuse) Write(header *raw.InHeader, input *raw.WriteIn, data []byte) (written uint32, code fuse.Status) {
	writer := m.openFiles.get(input.Fh).w
	fmt.Printf("Got writer %+v\n", writer)
	written = uint32(0)
	for written < input.Size {
		fmt.Printf("Writing from offset %d len %d\n", input.Offset+uint64(written), input.Size-written)
		n, err := writer.WriteAt(data, input.Offset+uint64(written), input.Size-written)
		written += n
		if err != nil {
			fmt.Printf("Error writing: %s \n", err.Error())
			return written, fuse.EROFS
		}
	}
	return written, fuse.OK
}

func (m *MaggieFuse) Flush(header *raw.InHeader, input *raw.FlushIn) fuse.Status {
	f := m.openFiles.get(input.Fh)
	if f.w != nil {
		err := f.w.Fsync()
		if err != nil {
			return fuse.EROFS
		}
	}
	return fuse.OK
}

func (m *MaggieFuse) Fsync(header *raw.InHeader, input *raw.FsyncIn) (code fuse.Status) {
	writer := m.openFiles.get(input.Fh).w
	err := writer.Fsync()
	if err != nil {
		return fuse.EROFS
	}
	return fuse.OK
}

type dentryWithName struct {
	name string
	maggiefs.Dentry
}

type dentrylist []dentryWithName

func (d dentrylist) Len() int           { return len(d) }
func (d dentrylist) Swap(i, j int)      { d[i], d[j] = d[j], d[i] }
func (d dentrylist) Less(i, j int) bool { return d[i].CreatedTime < d[j].CreatedTime }

func (m *MaggieFuse) ReadDir(l *fuse.DirEntryList, header *raw.InHeader, input *raw.ReadIn) fuse.Status {
	// read from map fd-> dirobject
	fmt.Printf("Called ReadDir with offset %d", input.Offset)
	dir, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return fuse.EROFS
	}
	fmt.Printf("dir.children %+v", dir.Children)
	// create sorted list
	entryList := dentrylist(make([]dentryWithName, len(dir.Children), len(dir.Children)))

	i := 0
	for name, entry := range dir.Children {
		entryList[i] = dentryWithName{name, entry}
		i++
	}
	sort.Sort(entryList)
	for i := int(input.Offset); i < len(entryList); i++ {
		inode, err := m.names.GetInode(entryList[i].Inodeid)
		if err != nil {
			return fuse.EROFS
		}
		if !l.Add(entryList[i].name, entryList[i].Inodeid, mode(inode.Ftype)) {
			break
		}
	}
	// fill until offset
	fmt.Printf("Readdir returning %+v", l)
	return fuse.OK
}

func (m *MaggieFuse) ReleaseDir(header *raw.InHeader, input *raw.ReleaseIn) {
	// noop, we do stateless dirs
}

func (m *MaggieFuse) FsyncDir(header *raw.InHeader, input *raw.FsyncIn) (code fuse.Status) {
	// unnecessary because we persist on all dir ops anyways
	return fuse.OK
}
