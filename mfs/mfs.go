package main

import (
  "fmt"
  "os"
  "flag"
  "github.com/jbooth/maggiefs/maggiefs"
  "github.com/jbooth/maggiefs/integration"
  "github.com/jbooth/maggiefs/client"
  "github.com/hanwen/go-fuse/fuse"
)

// usage:
// 
// mfs namenode propsFile
// mfs datanode propsFile
// mfs client namenodeAddr:port mountPoint

func usage() {
  fmt.Fprintf(os.Stderr, "usage: maggiefs [cmd]\n")
  fmt.Fprintf(os.Stderr, "maggiefs namenode path/to/propsFile\n")
  fmt.Fprintf(os.Stderr, "maggiefs datanode path/to/propsFile\n")
  fmt.Fprintf(os.Stderr, "maggiefs client namenodeAddr:port mountPoint\n")
  os.Exit(2)
}
//
//func doNameNode(pathToProps string) {
//  cfg,err := integration.NewName
//}

func doDataNode(pathToProps string) {
}

func doClient(pathToProps string) {
}



func main() {
  flag.Parse()
  args := flag.Args()
  fmt.Println(args)
  cmd := args[0]
  args = args[1:]
  switch cmd {
    
  }
  
  fmt.Println(baseDir)
  leases := maggiefs.NewLocalLeases()
  datas := maggiefs.NewLocalDatas(baseDir)
  names := maggiefs.NewMemNames(datas)
  mfs := client.NewMaggieFuse(leases,names,datas)
  fmt.Println(mfs)
  mountState := fuse.NewMountState(mfs)
  mountState.Debug = true
  err := mountState.Mount("/tmp/maggiefs",nil)
  if (err != nil) { fmt.Println(err) }
  fmt.Println("Mounted")
  mountState.Loop()
}
