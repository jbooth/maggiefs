// The fuse package provides APIs to implement filesystems in
// userspace.  Typically, each call of the API happens in its own
// goroutine, so take care to make the file system thread-safe.

package fuse

import (
	"github.com/jbooth/maggiefs/splice"
	"log"
)

type MountOptions struct {
	AllowOther bool

	// Options are passed as -o string to fusermount.
	Options []string

	// Default is _DEFAULT_BACKGROUND_TASKS, 12.  This numbers
	// controls the allowed number of requests that relate to
	// async I/O.  Concurrency for synchronous I/O is not limited.
	MaxBackground int

	// Write size to use.  If 0, use default. This number is
	// capped at the kernel maximum.
	MaxWrite int

	// If IgnoreSecurityLabels is set, all security related xattr
	// requests will return NO_DATA without passing through the
	// user defined filesystem.  You should only set this if you
	// file system implements extended attributes, and you are not
	// interested in security labels.
	IgnoreSecurityLabels bool // ignoring labels should be provided as a fusermount mount option.

	// If given, use this buffer pool instead of the global one.
	Buffers BufferPool

	// If RememberInodes is set, we will never forget inodes.
	// This may be useful for NFS.
	RememberInodes bool

	// The name will show up on the output of the mount. Keep this string
	// small.
	Name string

	// If set, wrap the file system in a single-threaded locking wrapper.
	SingleThreaded bool
}

// RawFileSystem is an interface close to the FUSE wire protocol.
//
// A null implementation is provided by NewDefaultRawFileSystem.
type RawFileSystem interface {
	String() string

	// If called, provide debug output through the log package.
	SetDebug(debug bool)

	Lookup(header *InHeader, name string, out *EntryOut) (status Status)
	Forget(nodeid, nlookup uint64)

	// Attributes.
	GetAttr(input *GetAttrIn, out *AttrOut) (code Status)
	SetAttr(input *SetAttrIn, out *AttrOut) (code Status)

	// Modifying structure.
	Mknod(input *MknodIn, name string, out *EntryOut) (code Status)
	Mkdir(input *MkdirIn, name string, out *EntryOut) (code Status)
	Unlink(header *InHeader, name string) (code Status)
	Rmdir(header *InHeader, name string) (code Status)
	Rename(input *RenameIn, oldName string, newName string) (code Status)
	Link(input *LinkIn, filename string, out *EntryOut) (code Status)
	Symlink(header *InHeader, pointedTo string, linkName string, out *EntryOut) (code Status)
	Readlink(header *InHeader) (out []byte, code Status)
	Access(input *AccessIn) (code Status)

	// Extended attributes.
	GetXAttrSize(header *InHeader, attr string) (sz int, code Status)
	GetXAttrData(header *InHeader, attr string) (data []byte, code Status)
	ListXAttr(header *InHeader) (attributes []byte, code Status)
	SetXAttr(input *SetXAttrIn, attr string, data []byte) Status
	RemoveXAttr(header *InHeader, attr string) (code Status)

	// File handling.
	Create(input *CreateIn, name string, out *CreateOut) (code Status)
	Open(input *OpenIn, out *OpenOut) (status Status)

	// note you must accurately report how many bytes you spliced into the pipe, if you cannot you should return an error status
	Read(input *ReadIn, buf ReadPipe) (code Status)

	Release(input *ReleaseIn)

	// receives
	Write(input *WriteIn, data []byte) (written uint32, code Status)

	Flush(input *FlushIn) Status
	Fsync(input *FsyncIn) (code Status)
	Fallocate(input *FallocateIn) (code Status)

	// Directory handling
	OpenDir(input *OpenIn, out *OpenOut) (status Status)
	ReadDir(input *ReadIn, out *DirEntryList) Status
	ReadDirPlus(input *ReadIn, out *DirEntryList) Status
	ReleaseDir(input *ReleaseIn)
	FsyncDir(input *FsyncIn) (code Status)

	//
	StatFs(input *InHeader, out *StatfsOut) (code Status)

	// This is called on processing the first request. The
	// filesystem implementation can use the server argument to
	// talk back to the kernel (through notify methods).
	Init(*Server)
}

// represents an OS pipe which read results will be piped through
type ReadPipe interface {
	// write the header prior to splicing any bytes -- code should be 0 on OK, or a syscall value like syscall.EIO on error
	WriteHeader(code int32, returnBytesLength int) error
	// write bytes from memory into this buffer
	Write(b []byte) (int, error)
	// splice bytes from the given fd into this buffer
	LoadFrom(fd uintptr, length int) (int, error)
	// splice bytes from the given fd at the given offset into this buffer
	LoadFromAt(fd uintptr, length int, offset int64) (int, error)
	// finish our read and write bytes to fuse channel
	Commit() error
}

type readPipe struct {
	fuseServer *Server
	req        *request
	pipe       *splice.Pair
	numInPipe  int
}

// func to write our header response prior to splicing bytes
// returnBytesLength should be the amount requested unless we are hitting EOF

// note that after calling this method, you MUST either write returnBytesLength to this pipe
// or return an error code from the Read() method
func (r *readPipe) WriteHeader(code int32, returnBytesLength int) error {
	r.req.status = Status(code)
	headerBytes := r.req.serializeHeader(returnBytesLength)

	r.numInPipe = len(headerBytes) + returnBytesLength
	err := r.pipe.Grow(r.numInPipe)
	if err != nil {
		log.Printf("Error growing pipe to size %d : %s", r.numInPipe, err.Error())
		r.req.status = EIO
		splice.Drop(r.pipe)
		r.pipe = nil
		return err
	}
	// response header goes in buffer first, before read results
	_, err = r.pipe.Write(headerBytes)
	if err != nil {
		log.Printf("Error writing header to pipe prior to read: %s\n", err)
		r.req.status = EIO
		splice.Drop(r.pipe)
		return err
	}
	return nil
}

func (r *readPipe) Write(b []byte) (int, error) {
	return r.pipe.Write(b)
}

func (r *readPipe) LoadFrom(fd uintptr, length int) (int, error) {
	return r.pipe.LoadFrom(fd, length)
}

func (r *readPipe) LoadFromAt(fd uintptr, length int, offset int64) (int, error) {
	return r.pipe.LoadFromAt(fd, length, offset)
}

func (r *readPipe) Commit() error {
	r.fuseServer.commitReadResults(r.req, r.pipe, r.numInPipe, nil)
	return nil
}
