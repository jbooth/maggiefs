// The fuse package provides APIs to implement filesystems in
// userspace.  Typically, each call of the API happens in its own
// goroutine, so take care to make the file system thread-safe.

package fuse

import (
	"time"

	"github.com/hanwen/go-fuse/raw"
)

// Types for users to implement.

// The result of Read is an array of bytes, but for performance
// reasons, we can also return data as a file-descriptor/offset/size
// tuple.  If the backing store for a file is another filesystem, this
// reduces the amount of copying between the kernel and the FUSE
// server.  The ReadResult interface captures both cases.
type ReadResult interface {
	// Returns the raw bytes for the read, possibly using the
	// passed buffer. The buffer should be larger than the return
	// value from Size.
	Bytes(buf []byte) []byte

	// Size returns how many bytes this return value takes at most.
	Size() int
}

// MountOptions contains time out options for a (Node)FileSystem.  The
// default copied from libfuse and set in NewMountOptions() is
// (1s,1s,0s).
type FileSystemOptions struct {
	EntryTimeout    time.Duration
	AttrTimeout     time.Duration
	NegativeTimeout time.Duration

	// If set, replace all uids with given UID.
	// NewFileSystemOptions() will set this to the daemon's
	// uid/gid.
	*Owner

	// If set, use a more portable, but slower inode number
	// generation scheme.  This will make inode numbers (exported
	// back to callers) stay within int32, which is necessary for
	// making stat() succeed in 32-bit programs.
	PortableInodes bool
}

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

	// The Name will show up on the output of the mount. Keep this string
	// small.
	Name string
}

// DefaultFileSystem implements a FileSystem that returns ENOSYS for every operation.
type DefaultFileSystem struct{}

// DefaultFile returns ENOSYS for every operation.
type DefaultFile struct{}

// RawFileSystem is an interface close to the FUSE wire protocol.
//
// Unless you really know what you are doing, you should not implement
// this, but rather the FileSystem interface; the details of getting
// interactions with open files, renames, and threading right etc. are
// somewhat tricky and not very interesting.
//
// Include DefaultRawFileSystem to inherit a null implementation.
type RawFileSystem interface {
	String() string

	Lookup(out *raw.EntryOut, header *raw.InHeader, name string) (status Status)
	Forget(nodeid, nlookup uint64)

	// Attributes.
	GetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.GetAttrIn) (code Status)
	SetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.SetAttrIn) (code Status)

	// Modifying structure.
	Mknod(out *raw.EntryOut, header *raw.InHeader, input *raw.MknodIn, name string) (code Status)
	Mkdir(out *raw.EntryOut, header *raw.InHeader, input *raw.MkdirIn, name string) (code Status)
	Unlink(header *raw.InHeader, name string) (code Status)
	Rmdir(header *raw.InHeader, name string) (code Status)
	Rename(header *raw.InHeader, input *raw.RenameIn, oldName string, newName string) (code Status)
	Link(out *raw.EntryOut, header *raw.InHeader, input *raw.LinkIn, filename string) (code Status)

	Symlink(out *raw.EntryOut, header *raw.InHeader, pointedTo string, linkName string) (code Status)
	Readlink(header *raw.InHeader) (out []byte, code Status)
	Access(header *raw.InHeader, input *raw.AccessIn) (code Status)

	// Extended attributes.
	GetXAttrSize(header *raw.InHeader, attr string) (sz int, code Status)
	GetXAttrData(header *raw.InHeader, attr string) (data []byte, code Status)
	ListXAttr(header *raw.InHeader) (attributes []byte, code Status)
	SetXAttr(header *raw.InHeader, input *raw.SetXAttrIn, attr string, data []byte) Status
	RemoveXAttr(header *raw.InHeader, attr string) (code Status)

	// File handling.
	Create(out *raw.CreateOut, header *raw.InHeader, input *raw.CreateIn, name string) (code Status)
	Open(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status Status)
	Read(*raw.InHeader, *raw.ReadIn, []byte) (ReadResult, Status)

	Release(header *raw.InHeader, input *raw.ReleaseIn)
	Write(*raw.InHeader, *raw.WriteIn, []byte) (written uint32, code Status)
	Flush(header *raw.InHeader, input *raw.FlushIn) Status
	Fsync(*raw.InHeader, *raw.FsyncIn) (code Status)

	// Directory handling
	OpenDir(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status Status)
	ReadDir(out *DirEntryList, header *raw.InHeader, input *raw.ReadIn) Status
	ReleaseDir(header *raw.InHeader, input *raw.ReleaseIn)
	FsyncDir(header *raw.InHeader, input *raw.FsyncIn) (code Status)

	//
	StatFs(out *StatfsOut, eader *raw.InHeader) (code Status)

	// Provide callbacks for pushing notifications to the kernel.
	Init(params *RawFsInit)
}

// DefaultRawFileSystem returns ENOSYS for every operation.
type DefaultRawFileSystem struct{}

// Talk back to FUSE.
//
// InodeNotify invalidates the information associated with the inode
// (ie. data cache, attributes, etc.)
//
// EntryNotify should be used if the existence status of an entry changes,
// (ie. to notify of creation or deletion of the file).
//
// Somewhat confusingly, InodeNotify for a file that stopped to exist
// will give the correct result for Lstat (ENOENT), but the kernel
// will still issue file Open() on the inode.
type RawFsInit struct {
	InodeNotify func(*raw.NotifyInvalInodeOut) Status
	EntryNotify func(parent uint64, name string) Status
}
