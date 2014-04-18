package main

import (
	"flag"
	"fmt"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/integration"
	"github.com/jbooth/maggiefs/nameserver"
	"log"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
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
	fmt.Fprintf(os.Stderr, "usage: mfs [-debug] [-cpuprofile <filePath>] [-blockprofile <filePath>] <cmd>\n")
	fmt.Fprintf(os.Stderr, "mfs master <path/to/config> : run a master\n")
	fmt.Fprintf(os.Stderr, "mfs peer <path/to/config> : run a peer\n")
	fmt.Fprintf(os.Stderr, "mfs client masterHostPort mountPoint : connect a non-peer client\n")
	fmt.Fprintf(os.Stderr, "mfs masterconfig <homedir> : prints defaulted master config to stdout, with homedir set as the master's home\n")
	fmt.Fprintf(os.Stderr, "mfs peerconfig [options] : prints peer config to stdout, run mfs peerconfig -usage to see options \n")
	fmt.Fprintf(os.Stderr, "mfs format <path/to/NameDataDir>: formats a directory for use as the master's homedir \n")
	fmt.Fprintf(os.Stderr, "mfs singlenode <numDNs> <volsPerDn> <replicationFactor> <baseDir> <mountPoint>\n")
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

	var running integration.Service = nil
	var err error
	cmd := args[0]
	// pop first instr
	args = args[1:]

	switch cmd {
	case "singlenode":
		running, err = singlenode(args)
		if err != nil {
			usage(err)
			return
		}
	case "peer":
		running, err = runPeer(args)
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
	case "client":
		// args are masterAddr, mountPoint
		running, err = runClient(args[0], args[1])
		if err != nil {
			usage(err)
			return
		}
	case "peerconfig":
		peercfg, err := peerConfig(args)
		if err != nil {
			usage(err)
			return
		}
		if peercfg != nil {
			peercfg.Write(os.Stdout)
		}
		return
	case "masterconfig":
		masterHome := "/tmp/mfsMaster"
		if len(args) > 0 {
			masterHome = args[0]
		}
		integration.DefaultMasterConfig(masterHome).Write(os.Stdout)
		return
	case "format":
		if len(args) < 1 {
			usage(fmt.Errorf("Usage: format [NameHome]"))
			return
		}
		fmt.Println("Formatting directory : ", args[0])
		err = nameserver.Format(args[0], uint32(os.Getuid()), uint32(os.Getgid()))
		if err != nil {
			usage(err)
			return
		}
		return
	default:
		usage(nil)
		return

	}
	// we didn't run one of the quick exits, so we have a running service and possibly mountpoint

	// this chan gets hit on error or signal
	errChan := make(chan error)

	// spin off service
	go func() {
		// catch panics and turn into channel send
		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic: %v\n", x)
				errChan <- fmt.Errorf("Run time panic: %v", x)
			}
		}()
		errChan <- running.Serve()
	}()

	// spin off signal handler
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGPIPE, syscall.SIGHUP)
	go func() {
		for {
			s := <-sig
			if s == syscall.SIGPIPE {
				fmt.Println("Got sigpipe!")
			} else {
				// silent quit for other signals
				errChan <- nil
				return
			}
		}
	}()

	// wait for something to come from either signal handler or the mountpoint, unmount and blow up safely
	err = <-errChan
	if running != nil {
		fmt.Println("Shutting down..")
		running.Close()
		running.WaitClosed()
	}
	if err != nil {
		panic(err)
	}
}

// we take the following args:
// -bindAddr string
// -nameHost string
// -mountPoint string
// -volRoots []string (comma delimited)
func peerConfig(args []string) (*integration.PeerConfig, error) {
	flagSet := flag.NewFlagSet("peerconfig", flag.ContinueOnError)
	var bindAddr, masterHost, mountPoint, volRootsStr string
	var usage bool
	volRoots := make([]string, 0)
	defaultHost, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	flagSet.StringVar(&bindAddr, "bindAddr", defaultHost, "The address that we broadcast to the cluster for connection")
	flagSet.StringVar(&masterHost, "masterAddr", defaultHost, "Address of the master")
	flagSet.StringVar(&mountPoint, "mountPoint", "/tmp/mfs", "Mountpoint for access to the MFS")
	flagSet.StringVar(&volRootsStr, "volRoots", "", "Optional comma-delimited list of paths to volumes that we're exporting to MFS")
	flagSet.BoolVar(&usage, "usage", false, "Prints usage and exits")
	err = flagSet.Parse(args)
	if err != nil {
		return nil, err
	}
	if usage {
		fmt.Println("Usage: mfs peerconfig [options]")
		flagSet.PrintDefaults()
		return nil, nil
	}
	if volRootsStr != "" {
		volRoots = strings.Split(volRootsStr, ",")
	}
	return integration.DefaultPeerConfig(bindAddr, masterHost, mountPoint, volRoots), nil
}

func runMaster(args []string) (s integration.Service, err error) {
	if len(args) < 1 {
		err = fmt.Errorf("Must run master with config:  mfs master [/path/to/config]")
		return
	}
	cfg := &integration.MasterConfig{}
	err = cfg.ReadConfig(args[0])
	if err != nil {
		return
	}
	fmt.Printf("Running master with config: \n %+v\n", cfg)
	s, err = integration.NewMaster(cfg, false)
	return
}

func singlenode(args []string) (s *integration.SingleNodeCluster, err error) {
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
	s, err = integration.NewSingleNodeCluster(11004, 1103, numDNs, volsPerDn, uint32(replicationFactor), baseDir, mountPoint, debug)
	return
}

func runPeer(args []string) (peer integration.Service, err error) {
	cfg := &integration.PeerConfig{}
	err = cfg.ReadConfig(args[0])
	if err != nil {
		return
	}
	peer, err = integration.NewPeer(cfg, debug)
	return
}

func runClient(masterHostPort string, mountPoint string) (integration.Service, error) {
	cli, err := integration.NewClient(masterHostPort)
	if err != nil {
		return nil, err
	}
	mfs, err := client.NewMaggieFuse(cli.Leases, cli.Names, cli.Datas, nil)
	if err != nil {
		return nil, err
	}
	mount, err := integration.NewMount(mfs, mountPoint, debug)
	return mount, err
}
