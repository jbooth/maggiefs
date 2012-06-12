package fuse

import (
  //"github.com/hanwen/go-fuse/raw"
  "github.com/hanwen/go-fuse/fuse"
)

type JFs struct {
  DefaultRawFileSystem

}

func (fs *JFs) GetAttr() {
  
}
