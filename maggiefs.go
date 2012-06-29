package maggiefs

import (
  "io"
)

const(
  PAGESIZE = uint32(4096)
  BLOCKSIZE = uint32(64 * 1024 * 1024)
)

type MaggieFs struct {
  names NameService
  datas DataService
}

func (fs *MaggieFs) String() string {
	return "MAGGIEFS"
}

func (fs *MaggieFs) Init() error {
  return nil
}

func (fs *MaggieFs) OpenR(path string)  (r io.ReadSeeker, err error) {
  return nil,nil
}

func (fs *MaggieFs) OpenW(path string) (w io.WriteSeeker, err error) {
  return nil,nil
}

func (fs *MaggieFs) OpenWR(path string) (rw io.ReadWriteSeeker, err error) {
  return nil,nil
}

func (fs *MaggieFs) Close() error {
  return nil
}
