package fuse

import (
  //"github.com/hanwen/go-fuse/raw"
  "github.com/hanwen/go-fuse/fuse"
)

type MaggieFs struct {

}


func (fs *MaggieFs) GetAttr(name string, context *fuse.Context) (*fuse.Attr, fuse.Status) {
    return nil, fuse.ENOSYS
}

func (fs *MaggieFs) GetXAttr(name string, attr string, context *fuse.Context) ([]byte, fuse.Status) {
    return nil, fuse.ENOSYS
}

func (fs *MaggieFs) SetXAttr(name string, attr string, data []byte, flags int, context *fuse.Context) fuse.Status {
    return fuse.ENOSYS
}

func (fs *MaggieFs) ListXAttr(name string, context *fuse.Context) ([]string, fuse.Status) {
    return nil, fuse.ENOSYS
}

func (fs *MaggieFs) RemoveXAttr(name string, attr string, context *fuse.Context) fuse.Status {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Readlink(name string, context *fuse.Context) (string, fuse.Status) {
    return "", fuse.ENOSYS
}

func (fs *MaggieFs) Mknod(name string, mode uint32, dev uint32, context *fuse.Context) fuse.Status {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Mkdir(name string, mode uint32, context *fuse.Context) fuse.Status {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Unlink(name string, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Rmdir(name string, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Symlink(value string, linkName string, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Rename(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Link(oldName string, newName string, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Chmod(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Chown(name string, uid uint32, gid uint32, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}
func (fs *MaggieFs) Truncate(name string, offset uint64, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Open(name string, flags uint32, context *fuse.Context) (file fuse.File, code fuse.Status) {
    return nil, fuse.ENOSYS
}

func (fs *MaggieFs) OpenDir(name string, context *fuse.Context) (stream []fuse.DirEntry, status fuse.Status) {
    return nil, fuse.ENOSYS
}

func (fs *MaggieFs) OnMount(nodeFs *PathNodeFs) {
}

func (fs *MaggieFs) OnUnmount() {
}

func (fs *MaggieFs) Access(name string, mode uint32, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}

func (fs *MaggieFs) Create(name string, flags uint32, mode uint32, context *fuse.Context) (file fuse.File, code fuse.Status) {
    return nil, fuse.ENOSYS
}

func (fs *MaggieFs) Utimens(name string, AtimeNs int64, CtimeNs int64, context *fuse.Context) (code fuse.Status) {
    return fuse.ENOSYS
}

func (fs *MaggieFs) String() string {
    return "MaggieFs"
}

func (fs *MaggieFs) StatFs(name string) *StatfsOut {
    return nil
}
