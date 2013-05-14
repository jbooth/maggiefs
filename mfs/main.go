package main

import (
  "fmt"
  "os"
  "flag"
  "github.com/jbooth/maggiefs/maggiefs"
  "github.com/jbooth/maggiefs/integration"
  "strconv"
  "github.com/jbooth/maggiefs/client"
  "github.com/hanwen/go-fuse/fuse"
)

// usage:
// 
// mfs namenode propsFile
// mfs datanode propsFile
// mfs client namenodeAddr:port leaseAddr:port mountPoint
// mfs singlenode numDNs volsPerDn replicationFactor baseDir

func usage(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n",err.Error())
	}
  fmt.Fprintf(os.Stderr, "usage: mfs [cmd]\n")
  fmt.Fprintf(os.Stderr, "mfs namenode path/to/propsFile\n")
  fmt.Fprintf(os.Stderr, "mfs datanode path/to/propsFile\n")
  fmt.Fprintf(os.Stderr, "mfs client namenodeAddr:port mountPoint\n")
  fmt.Fprintf(os.Stderr, "mfs singlenode numDNs volsPerDn replicationFactor baseDir mountPoint\n")
}
//
//func doNameNode(pathToProps string) {
//  cfg,err := integration.NewName
//}


func main() {
  flag.Parse()
  args := flag.Args()
  if len(args) < 1 {
  	usage(nil)
  	return
  }
  fmt.Println(args)
  cmd := args[0]
  args = args[1:]
  
  switch cmd {
  	case "singlenode":
  		numDNs,err := strconv.Atoi(args[0])
  		if err != nil {
  			usage(err)
  			return
  		}
  		volsPerDn,err := strconv.Atoi(args[1])
  		if err != nil {
  			usage(err)
  			return
  		}
  		replicationFactor,err := strconv.Atoi(args[2])
  		if err != nil {
  			usage(err)
  			return
  		}
  		baseDir := args[3]
  		mountPoint := args[4]
  		cluster,err := integration.NewSingleNodeCluster(numDNs, volsPerDn, uint32(replicationFactor), baseDir)
  		if err != nil {
  			usage(err)
  			return
  		}
  		client,err := newMountedClient(cluster.Leases,cluster.Names,cluster.Datas, mountPoint)
  		client.Loop()
  	default:
  		usage(nil)
  		return
  
  }
}

type mountedClient struct {
	ms fuse.MountState
	mountPoint string
}

func newMountedClient(leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService, mountPoint string) (*fuse.MountState,error) {
	mfs,err := client.NewMaggieFuse(leases,names,datas)
	if err != nil {
		return nil,err
	}
	mountState := fuse.NewMountState(mfs)
	mountState.Debug = true
	err = mountState.Mount(mountPoint,nil)
	return mountState,err
}
  
//  fmt.Println(baseDir)
//  leases := maggiefs.NewLocalLeases()
//  datas := maggiefs.NewLocalDatas(baseDir)
//  names := maggiefs.NewMemNames(datas)
//  mfs := client.NewMaggieFuse(leases,names,datas)
//  fmt.Println(mfs)
//  mountState := fuse.NewMountState(mfs)
//  mountState.Debug = true
//  err := mountState.Mount("/tmp/maggiefs",nil)
//  if (err != nil) { fmt.Println(err) }
//  fmt.Println("Mounted")
//  mountState.Loop()
