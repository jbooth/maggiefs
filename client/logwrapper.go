package client

import (
	"fmt"
	"github.com/hanwen/go-fuse/fuse"
	"github.com/hanwen/go-fuse/raw"
	"io"
)

type LoggingFs struct {
	w      io.Writer
	member fuse.RawFileSystem
}

func NewLoggingFs(w io.Writer, member fuse.RawFileSystem) fuse.RawFileSystem {
	return &LoggingFs{w, member}
}

func statString(stat fuse.Status) string {
	if stat == 0 {
		return "OK"
	}
	// TODO actually interpret error
	return "Error: " + string(stat)
}

func (f LoggingFs) String() string {
	return "logging fs wrapping " + f.member.String()
}

func (f LoggingFs) Lookup(out *raw.EntryOut, header *raw.InHeader, name string) (status fuse.Status) {
	fmt.Fprintf(f.w, "Lookup called with input header: %+v name: %s\n", header, name)
	stat := f.member.Lookup(out, header, name)
	fmt.Fprintf(f.w, "Lookup returned stat %s, entryOut %+v\n", statString(stat), out)
	return stat
}

func (f LoggingFs) Forget(nodeid, nlookup uint64) {
	//noop
}

func (f LoggingFs) GetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.GetAttrIn) (code fuse.Status) {
	fmt.Fprintf(f.w, "GetAttr called with input header: %+v, getAttrIn %+v\n", header, input)
	stat := f.member.GetAttr(out, header, input)
	fmt.Fprintf(f.w, "GetAttr returned stat %s, attrOut %+v\n", statString(stat), out)
	return stat
}

func (f LoggingFs) SetAttr(out *raw.AttrOut, header *raw.InHeader, input *raw.SetAttrIn) (code fuse.Status) {
	fmt.Fprintf(f.w, "SetAttr called with header %+v input %+v\n", header, input)
	code = f.member.SetAttr(out, header, input)
	fmt.Fprintf(f.w, "SetAttr returned stat %s, attrOut %+v\n", statString(code), out)
	return code
}

func (f LoggingFs) Mknod(out *raw.EntryOut, header *raw.InHeader, input *raw.MknodIn, name string) (code fuse.Status) {
	fmt.Fprintf(f.w, "Mknod called with header %+v input %+v name %s \n", header, input, name)
	stat := f.member.Mknod(out, header, input, name)
	fmt.Fprintf(f.w, "Mknod returned stat %s, entryOut %+v\n", statString(stat), out)
	return stat
}

func (f LoggingFs) Mkdir(out *raw.EntryOut, header *raw.InHeader, input *raw.MkdirIn, name string) (code fuse.Status) {
	fmt.Fprintf(f.w, "Mkdir called with header %+v input %+v name %s \n", header, input, name)
	stat := f.member.Mkdir(out, header, input, name)
	fmt.Fprintf(f.w, "Mkdir returned stat %s, entryOut %+v\n", statString(stat), out)
	return stat
}

func (f LoggingFs) Unlink(header *raw.InHeader, name string) (code fuse.Status) {
	fmt.Fprintf(f.w, "Mkdir called with header %+v name %s \n", header, name)
	stat := f.member.Unlink(header, name)
	fmt.Fprintf(f.w, "Mkdir returned stat %s \n", statString(stat))
	return stat
}

func (f LoggingFs) Rmdir(header *raw.InHeader, name string) (code fuse.Status) {
	fmt.Fprintf(f.w, "Rmdir called with header %+v name %s \n", header, name)
	stat := f.member.Rmdir(header, name)
	fmt.Fprintf(f.w, "Rmdir returned stat %s \n", statString(stat))
	return stat
}

func (f LoggingFs) Rename(header *raw.InHeader, input *raw.RenameIn, oldName string, newName string) (code fuse.Status) {
	fmt.Fprintf(f.w, "Rename called with header %+v input %+v oldName %s newName %s \n", header, input, oldName, newName)
	stat := f.member.Rename(header, input, oldName, newName)
	fmt.Fprintf(f.w, "Rename returned %s \n", statString(stat))
	return stat
}

func (f LoggingFs) Link(out *raw.EntryOut, header *raw.InHeader, input *raw.LinkIn, name string) (code fuse.Status) {
	fmt.Fprintf(f.w, "Link called with header %+v input %+v name %s \n", header, input, name)
	stat := f.member.Link(out, header, input, name)
	fmt.Fprintf(f.w, "Link returned stat %s, entryOut %+v\n", statString(stat), out)
	return stat
}

func (f LoggingFs) Symlink(out *raw.EntryOut, header *raw.InHeader, pointedTo string, linkName string) (code fuse.Status) {
	fmt.Fprintf(f.w, "Symlink called with header %+v pointedTo %s linkName %s\n", header, pointedTo, linkName)
	stat := f.member.Symlink(out, header, pointedTo, linkName)
	fmt.Fprintf(f.w, "Symlink returned stat %s out %+v\n", statString(stat), out)
	return stat
}

func (f LoggingFs) Readlink(header *raw.InHeader) (out []byte, code fuse.Status) {
	fmt.Fprintf(f.w, "Readlink called with header %+v\n", header)
	ret, stat := f.member.Readlink(header)
	fmt.Fprintf(f.w, "Readlink returned stat %s val %s\n", statString(stat), string(ret))
	return ret, stat
}

func (f LoggingFs) Access(header *raw.InHeader, input *raw.AccessIn) (code fuse.Status) {
	fmt.Fprintf(f.w, "Access called with header %+v input %+v\n", header, input)
	stat := f.member.Access(header, input)
	fmt.Fprintf(f.w, "Access returned stat %s\n", statString(stat))
	return stat
}

func (f LoggingFs) GetXAttrSize(header *raw.InHeader, attr string) (sz int, code fuse.Status) {
	fmt.Fprintf(f.w, "GetXattrSize called with header %+v attr %s\n", header, attr)
	sz, code = f.member.GetXAttrSize(header, attr)
	fmt.Fprintf(f.w, "GetXAttrSize returned sz %d stat %s", sz, statString(code))
	return sz, code
}

func (f LoggingFs) GetXAttrData(header *raw.InHeader, attr string) (data []byte, code fuse.Status) {
	fmt.Fprintf(f.w, "GetXattrData called with header %+v attr %s\n", header, attr)
	ret, code := f.member.GetXAttrData(header, attr)
	fmt.Fprintf(f.w, "GetXAttrData returned code %s value %s\n", statString(code), string(ret))
	return ret, code
}

func (f LoggingFs) ListXAttr(header *raw.InHeader) (attributes []byte, code fuse.Status) {
	fmt.Fprintf(f.w, "ListXattr called with header %+v\n", header)
	attributes, code = f.member.ListXAttr(header)
	fmt.Fprintf(f.w, "ListXAttr returned code %s value %s\n", statString(code), string(attributes))
	return attributes, code
}

func (f LoggingFs) SetXAttr(header *raw.InHeader, input *raw.SetXAttrIn, attr string, data []byte) fuse.Status {
	fmt.Fprintf(f.w, "SetXAttr called with header %+v input %+v attr %s data %s\n", header, input, attr, string(data))
	stat := f.member.SetXAttr(header, input, attr, data)
	fmt.Fprintf(f.w, "SetXattr returned stat %s\n", statString(stat))
	return stat
}

func (f LoggingFs) RemoveXAttr(header *raw.InHeader, attr string) (code fuse.Status) {
	fmt.Fprintf(f.w, "RemoveXAttr called with header %+v attrname %s\n", header, attr)
	code = f.member.RemoveXAttr(header, attr)
	fmt.Fprintf(f.w, "RemoveXAttr returned stat %s\n", statString(code))
	return code
}

func (f LoggingFs) Create(out *raw.CreateOut, header *raw.InHeader, input *raw.CreateIn, name string) (code fuse.Status) {
	fmt.Fprintf(f.w, "Create called with header %+v, input %+v, name %s\n", header, input, name)
	code = f.member.Create(out, header, input, name)
	fmt.Fprintf(f.w, "Create returned stat %s output %+v\n", statString(code), out)
	return code
}

func (f LoggingFs) Open(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
	fmt.Fprintf(f.w, "Open called with header %+v input %+v\n", header, input)
	status = f.member.Open(out, header, input)
	fmt.Fprintf(f.w, "Open returned stat %s output %+v\n", statString(status), out)
	return status
}

func (f LoggingFs) Read(header *raw.InHeader, readIn *raw.ReadIn, p []byte) (fuse.ReadResult, fuse.Status) {
	fmt.Fprintf(f.w, "Read called on header %+v readIn %+v\n", header, readIn)
	rr, stat := f.member.Read(header, readIn, p)
	fmt.Fprintf(f.w, "Read returned stat %s numRead %d\n", statString(stat), rr.Size())
	return rr, stat
}

func (f LoggingFs) Release(header *raw.InHeader, input *raw.ReleaseIn) {
	fmt.Fprintf(f.w, "Release called with header %+v input %+v\n", header, input)
	f.member.Release(header, input)
}

func (f LoggingFs) Write(header *raw.InHeader, writeIn *raw.WriteIn, p []byte) (written uint32, code fuse.Status) {
	fmt.Fprintf(f.w, "Write called on header %+v writeIn %+v\n", header, writeIn)
	written, code = f.member.Write(header, writeIn, p)
	fmt.Fprintf(f.w, "Write returned stat %s numWritten %d", statString(code), written)
	return written, code
}

func (f LoggingFs) Flush(header *raw.InHeader, input *raw.FlushIn) fuse.Status {
	fmt.Fprintf(f.w, "Flush called with header %+v input %+v\n", header, input)
	stat := f.member.Flush(header, input)
	fmt.Fprintf(f.w, "Flush returned stat %s\n", statString(stat))
	return stat
}

func (f LoggingFs) Fsync(header *raw.InHeader, input *raw.FsyncIn) fuse.Status {
	fmt.Fprintf(f.w, "Fsync called with header %+v input %+v\n", header, input)
	stat := f.member.Fsync(header, input)
	fmt.Fprintf(f.w, "Fsync returned stat %s\n", statString(stat))
	return stat
}

func (f LoggingFs) OpenDir(out *raw.OpenOut, header *raw.InHeader, input *raw.OpenIn) (status fuse.Status) {
	fmt.Fprintf(f.w, "OpenDir called with header %+v input %+v\n", header, input)
	stat := f.member.OpenDir(out, header, input)
	fmt.Fprintf(f.w, "OpenDir returned stat %s out %+v \n", statString(stat), out)
	return stat
}

func (f LoggingFs) ReadDir(out *fuse.DirEntryList, header *raw.InHeader, input *raw.ReadIn) (status fuse.Status) {
	fmt.Fprintf(f.w, "ReadDir called with header %+v input %+v\n", header, input)
	stat := f.member.ReadDir(out, header, input)
	fmt.Fprintf(f.w, "ReadDir returned stat %s out %+v \n", statString(stat), out)
	return stat
}

func (f LoggingFs) ReleaseDir(header *raw.InHeader, input *raw.ReleaseIn) {
	fmt.Fprintf(f.w, "ReleaseDir called with header %+v input %+v\n", header, input)
	f.member.ReleaseDir(header, input)
}

func (f LoggingFs) FsyncDir(header *raw.InHeader, input *raw.FsyncIn) fuse.Status {
	fmt.Fprintf(f.w, "FsyncDir called with header %+v input %+v\n", header, input)
	stat := f.member.FsyncDir(header, input)
	fmt.Fprintf(f.w, "FsyncDir returned stat %s\n", statString(stat))
	return stat
}

func (f LoggingFs) StatFs(out *fuse.StatfsOut, header *raw.InHeader) (code fuse.Status) {
	fmt.Fprintf(f.w, "StatFs called with header %+v\n", header)
	code = f.member.StatFs(out, header)
	fmt.Fprintf(f.w, "StatFs returned stat %s out %+v", statString(code), out)
	return code
}

func (f LoggingFs) Init(params *fuse.RawFsInit) {
	fmt.Fprintf(f.w, "Init called with params %+v\n", params)
	f.member.Init(params)
}
