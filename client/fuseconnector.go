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
	"sync/atomic"
	"syscall"
	"time"
)

const (
	PAGESIZE    = maggiefs.PAGESIZE
	BLOCKLENGTH = maggiefs.BLOCKLENGTH
)

var typeCheck fuse.RawFileSystem = &MaggieFuse{}

type MaggieFuse struct {
	leases         maggiefs.LeaseService
	names          maggiefs.NameService
	datas          maggiefs.DataService
	localDnId      *uint32
	openFiles      openFileMap                                // maps FD numbers to open files
	fdCounter      uint64                                     // used to get unique FD numbers
	changeNotifier chan maggiefs.NotifyEvent                  // remote changes to inodes are notified through this chan
	inodeNotify    func(node uint64, off int64, length int64) fuse.Status // used to signal to OS that a remote inode changed
	log            *log.Logger
}

// creates a new fuse adapter
// the provided localDnId is used to request local replicas of blocks when writing, it can be nil
func NewMaggieFuse(leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService, localDnId *uint32) (*MaggieFuse, error) {
	notifier := leases.GetNotifier()
	m := &MaggieFuse{
		leases,
		names,
		datas,
		localDnId,
		newOpenFileMap(10),
		uint64(0),
		notifier,
		nil,
		log.New(os.Stderr, "maggie-fuse", 0),
	}
	nc := NewNameCache(names, leases, func(n maggiefs.NotifyEvent) {
		stat := m.inodeNotify(n.Inodeid(),0,1<<63 - 1)
		if stat != fuse.OK {
			fmt.Printf("fuse got bad stat trying to notify host of change to inode %d\n",n.Inodeid())
		} else {
			fmt.Printf("fuse connector notified for inode %d, got value %+v", n.Inodeid(), stat)
		}
	})
	m.leases = nc
	m.names = nc
	return m, nil
}

func (m *MaggieFuse) mutate(inodeid uint64, mutator func(i *maggiefs.Inode) error) (*maggiefs.Inode, error) {
	wl, err := m.leases.WriteLease(inodeid)
	defer wl.Release()
	if err != nil {
		return nil, err
	}
	inode, err := m.names.GetInode(inodeid)
	if err != nil {
		return nil, err
	}
	err = mutator(inode)
	if err != nil {
		return nil, err
	}
	inode.Mtime = int64(time.Now().Unix())
	err = m.names.SetInode(inode)
	return inode, err
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
	fmt.Printf("Filled attrOut %v with inode %v\n",out,i)
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
	// TODO handle open flags

	// if read, readable = true

	// if write, then
	// if file length = 0, open fine
	// if file length > 0, we must be either TRUNC or APPEND
	// open flags
	readable, writable, truncate, appnd := parseWRFlags(input.Flags)

	//fmt.Printf("opening inode %d with flag %b, readable %t writable %t truncate %t append %t \n", input.InHeader.NodeId, input.Flags, readable, writable, truncate, appnd)

	// get inode
	inode, err := m.names.GetInode(input.InHeader.NodeId)
	if err != nil {
		return fuse.EROFS
	}
	fmt.Printf("Open got inode: %+v\n",inode)

	if truncate {
		// clear file before writing
	}

	if appnd {
		// make sure file writer is in append mode
	}

	// allocate new filehandle
	fh := atomic.AddUint64(&m.fdCounter, uint64(1))
	f := openFile{fh, inode.Inodeid, nil, nil, nil}
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
		f.w, err = NewInodeWriter(inode.Inodeid, m.leases, m.names, m.datas, m.localDnId)
		if err != nil {
			return fuse.EROFS
		}
	}

	// output
	out.Fh = fh
	out.OpenFlags = fuse.FOPEN_KEEP_CACHE
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

func (m *MaggieFuse) SetDebug(debug bool) {
	return
}

func (m *MaggieFuse) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	// if this is a truncate, handle the truncation separately from other mutations, it requires a special call
	if input.Valid&fuse.FATTR_SIZE != 0 {
		w, err := NewInodeWriter(input.InHeader.NodeId, m.leases, m.names, m.datas, m.localDnId)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fuseconnector: error opening writer to truncate inode %d to size %d : %s\n", input.InHeader.NodeId, input.Size, err.Error())
			return fuse.EROFS
		}
		err = w.Truncate(input.Size)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fuseconnector: error truncating inode %d to size %d : %s\n", input.InHeader.NodeId, input.Size, err.Error())
			return fuse.EROFS
		}
	}

	// other mutations, if applicable
	if input.Valid&(fuse.FATTR_MODE|fuse.FATTR_UID|fuse.FATTR_GID|fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW|fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW|fuse.FATTR_SIZE) != 0 {
		header := input.InHeader

		wl, err := m.leases.WriteLease(header.NodeId)
		defer wl.Release()
		if err != nil {
			return fuse.EROFS
		}
		inode, err := m.names.GetInode(header.NodeId)
		if err != nil {
			return fuse.EROFS
		}
		if input.Valid&fuse.FATTR_MODE != 0 {
			// chmod
			inode.Mode = uint32(07777) & input.Mode
		}
		if input.Valid&(fuse.FATTR_UID|fuse.FATTR_GID) != 0 {
			// chown
			inode.Uid = input.Uid
			inode.Gid = input.Gid
		}
		if input.Valid&(fuse.FATTR_MTIME|fuse.FATTR_MTIME_NOW) != 0 {
			// MTIME, not supporting ATIME
			if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
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
		fmt.Printf("SetAttr filling attr out with ino %+v\n", inode)
		fillAttrOut(out, inode)
	}

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
	_, err := m.mutate(input.InHeader.NodeId, func(node *maggiefs.Inode) error {
		node.Xattr[attr] = data
		return nil
	})
	if err != nil {
		return fuse.EROFS
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
	return b.Bytes(), fuse.ENOSYS
}

func (m *MaggieFuse) RemoveXAttr(header *fuse.InHeader, attr string) fuse.Status {
	_, err := m.mutate(header.NodeId, func(node *maggiefs.Inode) error {
		delete(node.Xattr, attr)
		return nil
	})
	if err != nil {
		return fuse.EROFS
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
		Mode:input.Mode, 
		Rdev: uint32(0), 
		Umask: input.Umask, 
		Padding: input.Padding}
	mknodOut := &fuse.EntryOut{}
	stat := m.Mknod(&mknodin, name, mknodOut)
	if !stat.Ok() {
		return stat
	}
	openInHeader := fuse.InHeader {
		NodeId: mknodOut.NodeId,	
	}
	
	openin := fuse.OpenIn{
		InHeader: openInHeader,
		Flags: input.Flags, 
		Unused: uint32(0),
	}
	// set header.NodeId to child, because that's what we actually open
	stat = m.Open(&openin, &out.OpenOut)
	if !stat.Ok() {
		return stat
	}
	// this is inefficient since we had inode in open.. oh well, we have a cache
	ino,err := m.names.GetInode(mknodOut.NodeId)
	if err != nil {
		fmt.Printf("Error getting ino we just CREATEd %s",err)
		return fuse.EROFS
	}
	fillEntryOut(&out.EntryOut,ino)
	return fuse.OK
}

func (m *MaggieFuse) OpenDir(input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	// noop, we do stateless dirs
	return fuse.OK
}

func (m *MaggieFuse) Read(input *fuse.ReadIn, buf fuse.ReadPipe) (fuse.Status) {
	reader := m.openFiles.get(input.Fh).r
	// reader.ReadAt buffers our response header and bytes into the supplied pipe
	err := reader.ReadAt(buf,input.Offset,input.Size)
	if err != nil {
		if err == io.EOF {
			return fuse.OK
		}
		return fuse.EIO
	} 
	return fuse.OK
}

func (m *MaggieFuse) Release(input *fuse.ReleaseIn) {
	f := m.openFiles.get(input.Fh)
	err := f.Close()
	if err != nil {
		m.log.Print("error closing file")
	}
}

func (m *MaggieFuse) Write(input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	fmt.Println("Call to write!")
	writer := m.openFiles.get(input.Fh).w
	//fmt.Printf("Got writer %+v\n", writer)
	written = uint32(0)
	for written < input.Size {
		//fmt.Printf("Writing from offset %d len %d\n", input.Offset+uint64(written), input.Size-written)
		n, err := writer.WriteAt(data, input.Offset+uint64(written), input.Size-written)
		written += n
		if err != nil {
			fmt.Printf("Error writing: %s \n", err.Error())
			return written, fuse.EROFS
		}
	}
	return written, fuse.OK
}

func (m *MaggieFuse) Flush(input *fuse.FlushIn) fuse.Status {
	f := m.openFiles.get(input.Fh)
	if f.w != nil {
		err := f.w.Fsync()
		if err != nil {
			return fuse.EROFS
		}
	}
	return fuse.OK
}

func (m *MaggieFuse) Fsync(input *fuse.FsyncIn) (code fuse.Status) {
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

func (m *MaggieFuse) ReadDir(input *fuse.ReadIn, l *fuse.DirEntryList) fuse.Status {
	// read from map fd-> dirobject
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
		success,_ := l.Add(nil,entryList[i].name, entryList[i].Inodeid, inode.FullMode())
		if !success {
			break
		}
	}
	return fuse.OK
}

func (m *MaggieFuse) ReadDirPlus(input *fuse.ReadIn, l *fuse.DirEntryList) fuse.Status {
	return m.ReadDir(input,l)
}
func (m *MaggieFuse) ReleaseDir(input *fuse.ReleaseIn) {
	// noop, we do stateless dirs
}

func (m *MaggieFuse) FsyncDir(input *fuse.FsyncIn) (code fuse.Status) {
	// unnecessary because we persist on all dir ops anyways
	return fuse.OK
}
