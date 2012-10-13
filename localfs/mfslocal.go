package main

import (
  "maggiefs"
  "fmt"
  "os"
  "flag"
  "github.com/hanwen/go-fuse/fuse"
)

func usage() {
  fmt.Fprintf(os.Stderr, "usage: maggiefs dataDir")
  os.Exit(2)
}
func main() {
  flag.Parse()
  args := flag.Args()
  fmt.Println(args)
  baseDir := args[0]
  fmt.Println(baseDir)
  datas := maggiefs.NewLocalDatas(baseDir)
  names := maggiefs.NewMemNames(datas)
  maggiefs := maggiefs.NewMaggieFuse(names,datas)
  fmt.Println(maggiefs)
  mountState := fuse.NewMountState(maggiefs)
  mountState.Debug = true
  err := mountState.Mount("/tmp/maggiefs",nil)
  if (err != nil) { fmt.Println(err) }
  fmt.Println("Mounted")
  mountState.Loop()
}
