package conf

import "fmt"

var (
	DEFAULT_DATABIND = fmt.Sprintf("0.0.0.0:%d", 1102)
	DEFAULT_WEBBIND  = fmt.Sprintf("0.0.0.0:%d", 1103)
)

// TODO
// change to masterAddr, namePort, leasePort
type PeerConfig struct {
	MasterAddr  string   `json: nameAddr`    // addr to connect to for master service, should be in host:PORT syntax
	BindAddr    string   `json: bindAddr`    // addr  we expose for our services in "x.x.x.x:PORT" syntax
	WebBindAddr string   `json: webBindAddr` // addr  we expose for our web services in "x.x.x.x:PORT" syntax, will be removed eventually
	VolumeRoots []string `json: volumeRoots` // list of paths to the roots of the volumes we're exposing
	MountPoint  string   `json: mountPoint`  // dir to mount to on the client machine, must exist prior to program run
}

func DefaultPeerConfig(bindAddr string, nameHost string, mountPoint string, volRoots []string) *PeerConfig {
	return &PeerConfig{
		fmt.Sprintf("%s:%d",nameHost,1102)
		DEFAULT_DATABIND,
		DEFAULT_WEBBIND,
		volRoots,
		mountPoint,
		2,
	}
}

type MasterConfig struct {
	BindAddr          string `json: bindAddr`          // host:port for nameservice and leaseservice
	WebBindAddr       string `json: webBindAddr`       // host:port for web interface
	NameHome          string `json: nameHome`          // path to nn home on disk
	ReplicationFactor uint32 `json: replicationFactor` // number of replicas for each block
}

func DefaultMasterConfig(nameHome string) *MasterConfig {
	return &MasterConfig{
		DEFAULT_DATABIND,
		DEFAULT_WEBBIND,
		nameHome,
		3,
	}
}

type FSConfig struct {
	ReplicationFactor uint32 `json: replicationFactor`
	BlockLength       uint32 `json: blockLength`
}