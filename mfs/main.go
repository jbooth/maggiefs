package main

import (
	"flag"
	"fmt"
	"github.com/jbooth/go-fuse/fuse"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/integration"
	"github.com/jbooth/maggiefs/nameserver"
	"github.com/jbooth/maggiefs/mrpc"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"syscall"
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
	fmt.Fprintf(os.Stderr, "mfs master path/to/config : run a master\n")
	fmt.Fprintf(os.Stderr, "mfs peer path/to/config : run a peer\n")
	fmt.Fprintf(os.Stderr, "mfs masterconfig [homedir] : prints default master config to stdout, with homedir set as the master's home\n")
	fmt.Fprintf(os.Stderr, "mfs peerconfig [masterHost] [mountPoint] [volRoots..] : prints default peer config to stdout \n")
	fmt.Fprintf(os.Stderr, "mfs format path/to/NameDataDir: formats a directory for use as the master's homedir \n")
	fmt.Fprintf(os.Stderr, "mfs singlenode numDNs volsPerDn replicationFactor baseDir mountPoint\n")
}

// main flags
var (
	debug        bool   = false
	cpuprofile   string = ""
	blockprofile string = ""
)

func init() {
	flag.BoolVar(&debug, "debug", false, "print debug info about which fuse operations we're doing and their errors")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "file to write CPU profiling information to")
	flag.StringVar(&blockprofile, "blockprofile", "", "file to write block profiling information to")
}

// running state
var ()

// run
func main() {
	flag.Parse()
	args := flag.Args()
	if cpuprofile != "" {
		fmt.Printf("cpuprof file: %s\n", cpuprofile)
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}
	if blockprofile != "" {
		f, err := os.Create(blockprofile)
		if err != nil {
			log.Fatal(err)
		}
		runtime.SetBlockProfileRate(1)
		defer func() {
			pprof.Lookup("block").WriteTo(f, 0)
		}()
	}

	if len(args) < 1 {
		usage(nil)
		return
	}

	var running mrpc.Service = nil
	var mount *mountedClient = nil
	var err error
	cmd := args[0]
	// pop first instr
	args = args[1:]

	switch cmd {
	case "singlenode":
		running, mount, err = singlenode(args)
		if err != nil {
			usage(err)
			return
		}
	case "peer":
		running, mount, err = runPeer(args)
		if err != nil {
			usage(err)
			return
		}
	case "master":
		running, err = runMaster(args)
		if err != nil {
			usage(err)
			return
		}
	case "masterconfig":
		masterHome := "/tmp/mfsMaster"
		if len(args) > 0 {
			masterHome = args[0]
		}
		conf.DefaultMasterConfig(masterHome).Write(os.Stdout)
		return
	case "format":
		if len(args) < 1 {
			usage(fmt.Errorf("Usage: format [NameHome]"))
			return
		}
		err = nameserver.Format(args[0],uint32(os.Getuid()),uint32(os.Getgid()))
		if err != nil {
			panic(err)
		}
		return
	case "peerconfig":
		// writes a dataconfig to std out
		// args are
		// 1) host of namenode
		// 2) mountPoint
		// 3) []paths to DN volumeRoots on the datanode
		var masterHost = "localhost"
		if len(args) > 0 {
			masterHost = args[0]
		}
		mountPoint := "/tmp/mfsMount"
		if len(args) > 1 {
			mountPoint = args[1]
		}
		volRoots := make([]string, 0)
		if len(args) > 2 {
			volRoots = args[2:]
		}
		conf.DefaultPeerConfig(masterHost, mountPoint, volRoots).Write(os.Stdout)
		return
	default:
		usage(nil)
		return

	}
	// we didn't run one of the quick exits, so we have a running service and possibly mountpoint

	// this chan gets hit on error or signal
	errChan := make(chan error)

	// spin off fuse mountpoint
	go func() {

		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic: %v\n", x)
				errChan <- fmt.Errorf("Run time panic: %v", x)
			}
		}()
		// either run mountpoint service or just wait while master service runs
		if (mount != nil && mount.ms != nil) {
			mount.ms.Loop()
		} else {
			waitForever := make(chan bool)
			b := <- waitForever
		}
	}()

	// spin off signal handler
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGPIPE)
	go func() {
		s := <-sig
		if s == syscall.SIGPIPE {
			errChan <- fmt.Errorf("SIGPIPE")
		} else {
			// silent quit for other signals
			errChan <- nil
		}
	}()

	// wait for something to come from either signal handler or the mountpoint, unmount and blow up safely
	err = <-errChan
	if mount != nil && mount.ms != nil {
		mount.ms.Unmount()
	}
	if running != nil {
		running.Close()
		running.WaitClosed()
	}
	if err != nil {
		panic(err)
	}
}

func runMaster(args []string) (s mrpc.Service, err error) {
	if len(args) < 1 {
		err = fmt.Errorf("Must run master with config:  mfs master [/path/to/config]")
		return
	}
	cfg := &conf.MasterConfig{}
	err = cfg.ReadConfig(args[0])
	if err != nil {
		return
	}
	fmt.Printf("Running master with config: \n %+v\n", cfg)
	s, err = integration.NewNameServer(cfg, false)
	return
}

func singlenode(args []string) (s *integration.SingleNodeCluster, c *mountedClient, err error) {
	numDNs, err := strconv.Atoi(args[0])
	if err != nil {
		return
	}
	volsPerDn, err := strconv.Atoi(args[1])
	if err != nil {
		return
	}
	replicationFactor, err := strconv.Atoi(args[2])
	if err != nil {
		return
	}
	baseDir := args[3]
	mountPoint := args[4]
	nncfg, dscfg, err := conf.NewConfSet2(numDNs, volsPerDn, uint32(replicationFactor), baseDir)
	if err != nil {
		return
	}
	s, err = integration.NewSingleNodeCluster(nncfg, dscfg, true)
	if err != nil {
		return
	}
	err = s.Start()
	if err != nil {
		return
	}
	c, err = newMountedClient(s.FuseConnector, mountPoint)
	return
}

func runPeer(args []string) (s *integration.Peer, c *mountedClient, err error) {
	cfg := &conf.PeerConfig{}
	err = cfg.ReadConfig(args[0])
	if err != nil {
		return
	}
	s, err = integration.NewPeer(cfg)
	if err != nil {
		return
	}
	err = s.Start()
	if err != nil {
		return
	}
	c, err = newMountedClient(s.FuseConnector, cfg.MountPoint)
	return
}

type mountedClient struct {
	ms         *fuse.MountState
	mountPoint string
}

func newMountedClient(mfs fuse.RawFileSystem, mountPoint string) (*mountedClient, error) {
	mountState := fuse.NewMountState(mfs)

	mountState.Debug = debug
	opts := &fuse.MountOptions{
		MaxBackground: 12,
		//Options: []string {"ac_attr_timeout=0"},//,"attr_timeout=0","entry_timeout=0"},
	}
	err := mountState.Mount(mountPoint, opts)
	return &mountedClient{mountState, mountPoint}, err
}
