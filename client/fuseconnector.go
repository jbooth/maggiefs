package client

import (
	"bytes"
	"fmt"
	"github.com/jbooth/maggiefs/fuse"
	"github.com/jbooth/maggiefs/maggiefs"
	"io"
	"log"
	"os"
	"sort"
	"syscall"
	"time"
)

const (
	PAGESIZE    = maggiefs.PAGESIZE
	BLOCKLENGTH = maggiefs.BLOCKLENGTH
)

var typeCheck fuse.RawFileSystem = &MaggieFuse{}

type MaggieFuse struct {
	leases      maggiefs.LeaseService
	names       maggiefs.NameService
	datas       maggiefs.DataService
	localDnId   *uint32
	openFiles   *OpenFileMap                                           // maps FD numbers to open files
	fdCounter   uint64                                                 // used to get unique FD numbers
	inodeNotify func(node uint64, off int64, length int64) fuse.Status // used to signal to OS that a remote inode changed
	log         *log.Logger
}

// creates a new fuse adapter
// the provided localDnId is used to request local replicas of blocks when writing, it can be nil
func NewMaggieFuse(leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService, localDnId *uint32) (*MaggieFuse, error) {
	m := &MaggieFuse{
		leases,
		names,
		datas,
		localDnId,
		nil,
		uint64(0),
		nil,
		log.New(os.Stderr, "maggie-fuse", 0),
	}
	of := NewOpenFileMap(leases, names, datas, localDnId, func(inodeid uint64) {
		if m.inodeNotify == nil {
			log.Printf("Warning, call to notify before MaggieFuse.Init!")
			return
		}
		stat := m.inodeNotify(inodeid, 0, 1<<63-1)
		if stat != fuse.OK {
			log.Printf("fuse got bad stat trying to notify host of change to inode %d\n", inodeid)
		} else {
			log.Printf("fuse connector notified for inode %d, got value %+v", inodeid, stat)
		}
	})
	m.openFiles = of
	return m, nil
}

// FUSE implementation
func (m *MaggieFuse) Init(init *fuse.Server) {
	m.inodeNotify = init.InodeNotify
}

func (m *MaggieFuse) String() string {
	return "MAGGIEFS"
}

func (m *MaggieFuse) StatFs(h *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
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

func numBlocks(size uint64, blksize uint32) uint64 {
	bs := uint64(blksize)
	leftover := size % bs
	numB := size / bs
	if leftover != 0 {
		numB++
	}
	return numB
}

func (m *MaggieFuse) Lookup(h *fuse.InHeader, name string, out *fuse.EntryOut) (code fuse.Status) {
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

func fillEntryOut(out *fuse.EntryOut, i *maggiefs.Inode) {
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
	out.Mode = i.FullMode()
	out.Nlink = i.Nlink
	out.Uid = i.Uid
	out.Gid = i.Gid
	out.Rdev = uint32(0) // regular file, not block dvice
	out.Blksize = maggiefs.PAGESIZE
}

func (m *MaggieFuse) Forget(nodeID, nlookup uint64) {
	// noop
}

func fillAttrOut(out *fuse.AttrOut, i *maggiefs.Inode) {
	// fuse.Attr
	out.Ino = i.Inodeid
	out.Size = i.Length
	out.Blocks = numBlocks(i.Length, maggiefs.PAGESIZE)
	out.Atime = uint64(0)       // always 0 for atime
	out.Mtime = uint64(i.Mtime) // Mtime is user modifiable and is the last time data changed
	out.Ctime = uint64(i.Ctime) // Ctime is tracked by the FS and changes when attrs or data change
	out.Atimensec = uint32(0)
	out.Mtimensec = uint32(0)
	out.Ctimensec = uint32(0)
	out.Mode = i.FullMode()
	out.Nlink = i.Nlink
	out.Uid = i.Uid
	out.Gid = i.Gid
	out.Rdev = uint32(0) // regular file, not block dvice
	out.Blksize = maggiefs.PAGESIZE
	// fuse.AttrOut
	out.AttrValid = uint64(0)
	out.AttrValidNsec = uint32(100)
}

func (m *MaggieFuse) GetAttr(input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	i, err := m.names.GetInode(input.InHeader.NodeId)
	if err != nil {
		return fuse.EROFS
	}
	fillAttrOut(out, i)
	return fuse.OK
}

func (m *MaggieFuse) Open(input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	// open flags
	_, writable, truncate, appnd := parseWRFlags(input.Flags)

	if truncate {
		// clear file before writing
		_, err := m.names.Truncate(input.InHeader.NodeId, 0)
		if err != nil {
			log.Printf("Error truncating from Open: %s", err)
		}
	}

	if appnd {
		writable = true // do we need to do anything here?
	}
	// allocate new filehandle
	fh, err := m.openFiles.Open(input.InHeader.NodeId, writable)
	if err != nil {
		return fuse.ENOSYS
	}
	// output
	out.Fh = fh
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
	log.Printf("opened inode %d with fh %d", input.InHeader.NodeId, fh)
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

func (m *MaggieFuse) SetDebug(debug bool) {
	return
}

type SetAttr struct {
	SetMode  bool
	Mode     uint32
	SetUid   bool
	Uid      uint32
	SetGid   bool
	Gid      uint32
	SetMtime bool
	Mtime    uint32
}

func (m *MaggieFuse) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	var ino *maggiefs.Inode
	var err error
	// truncate if appropriate
	if input.Valid&fuse.FATTR_SIZE != 0 {
		ino, err = m.names.Truncate(input.NodeId, input.Size)
		if err != nil {
			log.Printf("Error truncating from SetAttr: %s", err)
		}
	}
	// only make the call to SetAttr if we have a valid param
	if input.Valid&(fuse.FATTR_MODE|fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW|fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW) != 0 {

		set := maggiefs.SetAttr{
			SetMode:  input.Valid&(fuse.FATTR_MODE) != 0,
			Mode:     input.Mode,
			SetUid:   input.Valid&(fuse.FATTR_UID) != 0,
			Uid:      input.Uid,
			SetGid:   input.Valid&(fuse.FATTR_GID) != 0,
			Gid:      input.Gid,
			SetMtime: input.Valid&(fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW) != 0,
			Mtime:    input.Mtime,
		}
		ino, err = m.names.SetAttr(input.NodeId, set)
		if err != nil {
			log.Printf("Error set attr from setAttr with arg %+v : %s", set, err)
		}
	}
	if ino == nil {
		ino, err = m.names.GetInode(input.NodeId)
		if err != nil {
			log.Printf("Error getting ino for SetAttr return: %s", err)
		}
	}
	fmt.Printf("SetAttr filling attr out with ino %+v\n", ino)
	fillAttrOut(out, ino)
	return fuse.OK
}

func (m *MaggieFuse) Readlink(header *fuse.InHeader) (out []byte, code fuse.Status) {
	// read string destination path for a symlink
	symlink, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return nil, fuse.EROFS
	}
	return []byte(symlink.Symlinkdest), fuse.OK
}

func (m *MaggieFuse) Mknod(input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {

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
		input.InHeader.Uid,
		input.InHeader.Gid,
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
	err = m.names.Link(input.InHeader.NodeId, i.Inodeid, name, false)
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

func (m *MaggieFuse) Mkdir(input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {

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
		input.InHeader.Uid,
		input.InHeader.Gid,
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
	err = m.names.Link(input.InHeader.NodeId, id, name, false)
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

func (m *MaggieFuse) Unlink(header *fuse.InHeader, name string) (code fuse.Status) {

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

func (m *MaggieFuse) Rmdir(header *fuse.InHeader, name string) (code fuse.Status) {
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

func (m *MaggieFuse) Symlink(header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
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

func (m *MaggieFuse) Rename(input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	// look up old parent and get our inode
	oldParent, err := m.names.GetInode(input.InHeader.NodeId)
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

func (m *MaggieFuse) Link(input *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	// new parent is header.NodeId
	// existing node is input.Oldnodeid

	// add link to new parent
	err := m.names.Link(input.InHeader.NodeId, input.Oldnodeid, name, false)
	if err == maggiefs.E_EXISTS {
		return fuse.Status(syscall.EEXIST)
	} else if err != nil {
		return fuse.EROFS
	}

	return fuse.OK
}

func (m *MaggieFuse) GetXAttrSize(header *fuse.InHeader, attr string) (size int, code fuse.Status) {
	node, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return 0, fuse.EROFS
	}

	return len(node.Xattr), fuse.OK
}

func (m *MaggieFuse) GetXAttrData(header *fuse.InHeader, attr string) (data []byte, code fuse.Status) {
	node, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return nil, fuse.EROFS
	}

	// punt
	return node.Xattr[attr], fuse.OK
}

func (m *MaggieFuse) SetXAttr(input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	err := m.names.SetXAttr(input.InHeader.NodeId, []byte(attr), data)
	if err != nil {
		return fuse.ENOSYS
	}
	// punt
	return fuse.OK
}

func (m *MaggieFuse) ListXAttr(header *fuse.InHeader) (data []byte, code fuse.Status) {
	node, err := m.names.GetInode(header.NodeId)
	if err != nil {
		return nil, fuse.EROFS
	}
	b := bytes.NewBuffer([]byte{})
	for k, _ := range node.Xattr {
		b.Write([]byte(k))
		b.WriteByte(0)
	}
	return b.Bytes(), fuse.OK
}

func (m *MaggieFuse) RemoveXAttr(header *fuse.InHeader, attr string) fuse.Status {
	err := m.names.DelXAttr(header.NodeId, []byte(attr))
	if err != nil {
		log.Printf("Error deleting XAttr %s from inode %d", attr, header.NodeId)
		return fuse.ENOSYS
	}
	return fuse.OK
}

// this is an optimization call -- can just return OK for now
func (m *MaggieFuse) Fallocate(input *fuse.FallocateIn) (code fuse.Status) {
	// TODO implement, Fallocate should write 0s to the allocated range
	return fuse.OK
}

func (m *MaggieFuse) Access(input *fuse.AccessIn) (code fuse.Status) {
	// check perms, always return ok
	// we're not doing perms yet handle later
	return fuse.OK
}

func (m *MaggieFuse) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	header := input.InHeader
	// call mknod and then open

	mknodin := fuse.MknodIn{
		InHeader: header,
		Mode:     input.Mode,
		Rdev:     uint32(0),
		Umask:    input.Umask,
		Padding:  input.Padding}
	mknodOut := &fuse.EntryOut{}
	stat := m.Mknod(&mknodin, name, mknodOut)
	if !stat.Ok() {
		return stat
	}
	openInHeader := fuse.InHeader{
		NodeId: mknodOut.NodeId,
	}

	openin := fuse.OpenIn{
		InHeader: openInHeader,
		Flags:    input.Flags,
		Unused:   uint32(0),
	}
	// set header.NodeId to child, because that's what we actually open
	stat = m.Open(&openin, &out.OpenOut)
	if !stat.Ok() {
		return stat
	}
	// get inode from the open files map to save a round-trip
	_, ino, err := m.openFiles.getInode(out.OpenOut.Fh)
	if err != nil {
		fmt.Printf("Error getting ino we just CREATEd %s", err)
		return fuse.EROFS
	}
	fillEntryOut(&out.EntryOut, ino)
	return fuse.OK
}

func (m *MaggieFuse) OpenDir(input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	// noop, we do stateless dirs
	return fuse.OK
}

func (m *MaggieFuse) Read(input *fuse.ReadIn, buf fuse.ReadPipe) fuse.Status {
	err := m.openFiles.Read(input.Fh, buf, input.Offset, input.Size)
	if err != nil {
		if err == io.EOF {
			return fuse.OK
		}
		log.Printf("Error reading from fd %d : %s", input.Fh, buf)
		return fuse.EIO
	}
	return fuse.OK
}

func (m *MaggieFuse) Release(input *fuse.ReleaseIn) {
	err := m.openFiles.Close(input.Fh)
	if err != nil {
		log.Printf("error closing file")
	}
}

func (m *MaggieFuse) Write(input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	nWritten, err := m.openFiles.Write(input.Fh, data, input.Offset, input.Size)
	if err != nil {
		log.Printf("Error writing bytes to fh %d : %s", input.Fh, err)
		return 0, fuse.EIO
	}
	return nWritten, fuse.OK
}

func (m *MaggieFuse) Flush(input *fuse.FlushIn) fuse.Status {
	err := m.openFiles.Sync(input.Fh)
	if err != nil {
		log.Printf("Error syncing fd %d : %s", input.Fh, err)
		return fuse.EIO
	}
	return fuse.OK
}

func (m *MaggieFuse) Fsync(input *fuse.FsyncIn) (code fuse.Status) {
	err := m.openFiles.Sync(input.Fh)
	if err != nil {
		log.Printf("Error syncing fd %d : %s", input.Fh, err)
		return fuse.EIO
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

func (m *MaggieFuse) ReadDir(input *fuse.ReadIn, l *fuse.DirEntryList) fuse.Status {
	dir, err := m.names.GetInode(input.InHeader.NodeId)
	if err != nil {
		return fuse.EINVAL
	}
	// create sorted list
	entryList := dentrylist(make([]dentryWithName, len(dir.Children), len(dir.Children)))

	i := 0
	for name, entry := range dir.Children {
		entryList[i] = dentryWithName{name, entry}
		i++
	}
	sort.Sort(entryList)
	// fill until offset
	for i := int(input.Offset); i < len(entryList); i++ {
		inode, err := m.names.GetInode(entryList[i].Inodeid)
		if err != nil {
			return fuse.EROFS
		}
		success, _ := l.Add(nil, entryList[i].name, entryList[i].Inodeid, inode.FullMode())
		if !success {
			break
		}
	}
	return fuse.OK
}

func (m *MaggieFuse) ReadDirPlus(input *fuse.ReadIn, l *fuse.DirEntryList) fuse.Status {
	dir, err := m.names.GetInode(input.InHeader.NodeId)
	if err != nil {
		return fuse.EINVAL
	}
	// create sorted list
	entryList := dentrylist(make([]dentryWithName, len(dir.Children), len(dir.Children)))

	i := 0
	for name, entry := range dir.Children {
		entryList[i] = dentryWithName{name, entry}
		i++
	}
	sort.Sort(entryList)

	// fill until offset
	d := fuse.DirEntry{}
	e := &fuse.EntryOut{}
	for i := int(input.Offset); i < len(entryList); i++ {
		inode, err := m.names.GetInode(entryList[i].Inodeid)
		if err != nil {
			return fuse.EROFS
		}
		d.Name = entryList[i].name
		d.Mode = inode.FullMode()
		fillEntryOut(e, inode)
		success, _ := l.AddDirLookupEntry(d, e)
		if !success {
			break
		}
	}
	return fuse.OK
}
func (m *MaggieFuse) ReleaseDir(input *fuse.ReleaseIn) {
	// noop, we do stateless dirs
}

func (m *MaggieFuse) FsyncDir(input *fuse.FsyncIn) (code fuse.Status) {
	// unnecessary because we persist on all dir ops anyways
	return fuse.OK
}
