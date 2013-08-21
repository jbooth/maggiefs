package main

import (
	"flag"
	"fmt"
	"github.com/jbooth/go-fuse/fuse"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/integration"
	"github.com/jbooth/maggiefs/maggiefs"
	"os"
	"strconv"
)

// usage:
//
// mfs namenode propsFile
// mfs datanode propsFile
// mfs client namenodeAddr:port leaseAddr:port mountPoint
// mfs singlenode numDNs volsPerDn replicationFactor baseDir

func usage(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s\n", err.Error())
	}
	fmt.Fprintf(os.Stderr, "usage: mfs [cmd]\n")
	fmt.Fprintf(os.Stderr, "mfs namenode path/to/propsFile\n")
	fmt.Fprintf(os.Stderr, "mfs datanode path/to/propsFile [mountPoint]\n")
	fmt.Fprintf(os.Stderr, "mfs client namenodeAddr:port leaseAddr:port mountPoint\n")
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
	// pop first instr
	args = args[1:]

	switch cmd {
	case "singlenode":
		singlenode(args)
	case "dataserver":
		runDataserver(args)
	case "nameserver":
		runNameserver(args)
	case "nameconfig":
		conf.DefaultNSConfig(args[0]).Write(os.Stdout)
		return
	case "dataconfig":
		// args are nameHost, vol1, vol2, volN...
		conf.DefaultDSConfig(args[0], args[1:]).Write(os.Stdout)
		// TODO actually set up configured homedir rather than just printing
	default:
		usage(nil)
		return

	}
}

func runNameserver(args []string) {
	cfg := &conf.NSConfig{}
	err := cfg.ReadConfig(args[0])
	if err != nil {
		usage(err)
		return
	}
	fmt.Printf("%+v\n", cfg)
	format := (len(args) > 1 && args[1] == "-format")
	ns, err := integration.NewNameServer(cfg, format)
	if err != nil {
		usage(err)
		return
	}
	ns.Start()
	ns.WaitClosed()
}

func singlenode(args []string) {
	numDNs, err := strconv.Atoi(args[0])
	if err != nil {
		usage(err)
		return
	}
	volsPerDn, err := strconv.Atoi(args[1])
	if err != nil {
		usage(err)
		return
	}
	replicationFactor, err := strconv.Atoi(args[2])
	if err != nil {
		usage(err)
		return
	}
	baseDir := args[3]
	mountPoint := args[4]
	nncfg, dscfg, err := conf.NewConfSet2(numDNs, volsPerDn, uint32(replicationFactor), baseDir)
	if err != nil {
		usage(err)
		return
	}
	cluster, err := integration.NewSingleNodeCluster(nncfg, dscfg, true)
	if err != nil {
		usage(err)
		return
	}
	cluster.Start()
	client, err := newMountedClient(cluster.Leases, cluster.Names, cluster.Datas, mountPoint)
	client.Loop()
	cluster.Close()
}

func runDataserver(args []string) {
	cfg := &conf.DSConfig{}
	err := cfg.ReadConfig(args[0])
	if err != nil {
		usage(err)
		return
	}
	services, err := integration.NewClient(cfg.NameAddr, cfg.LeaseAddr, 1)
	if err != nil {
		usage(err)
		return
	}
	ds, err := dataserver.NewDataServer(cfg.VolumeRoots, cfg.DataClientBindAddr, cfg.NameDataBindAddr, cfg.WebBindAddr, services.Names, services.Datas)
	if err != nil {
		usage(err)
		return
	}
	ds.Start()
	if len(args) > 1 {
		mountPoint := args[1]

		// start client
		client, err := newMountedClient(services.Leases, services.Names, services.Datas, mountPoint)
		if err != nil {
			usage(err)
			return
		}
		client.Loop()
		ds.Close()
	}
	ds.WaitClosed()
}

type mountedClient struct {
	ms         fuse.MountState
	mountPoint string
}

func newMountedClient(leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService, mountPoint string) (*fuse.MountState, error) {
	mfs, err := client.NewMaggieFuse(leases, names, datas)
	if err != nil {
		return nil, err
	}
	mountState := fuse.NewMountState(mfs)

	mountState.Debug = true
	opts := &fuse.MountOptions{
		MaxBackground: 12,
		//Options: []string {"max_read=131072", "max_readahead=131072","max_write=131072"},
	}
	err = mountState.Mount(mountPoint, opts)
	return mountState, err
}
