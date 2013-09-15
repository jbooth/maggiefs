package conf

import "fmt"

const (
	DEFAULT_LEASESERVER_PORT = 1101
	DEFAULT_NAMESERVER_PORT  = 1102
	DEFAULT_NAMEWEB_PORT     = 1103

	DEFAULT_DATA_PORT     = 1104
	DEFAULT_NAMEDATA_PORT = 1105
	DEFAULT_DATAWEB_PORT  = 1106
)



var (
	DEFAULT_LEASEADDR     = fmt.Sprintf("0.0.0.0:%d", DEFAULT_LEASESERVER_PORT)
	DEFAULT_NAMEADDR      = fmt.Sprintf("0.0.0.0:%d", DEFAULT_NAMESERVER_PORT)
	DEFAULT_NAMEWEB_ADDR  = fmt.Sprintf("0.0.0.0:%d", DEFAULT_NAMEWEB_PORT)
	DEFAULT_DATA_ADDR     = fmt.Sprintf("0.0.0.0:%d", DEFAULT_DATA_PORT)
	DEFAULT_NAMEDATA_ADDR = fmt.Sprintf("0.0.0.0:%d", DEFAULT_NAMEDATA_PORT)
	DEFAULT_DATAWEB_ADDR  = fmt.Sprintf("0.0.0.0:%d", DEFAULT_DATAWEB_PORT)
)

// TODO
// change to masterAddr, namePort, leasePort
type PeerConfig struct {
	LeaseAddr          string   `json: leaseAddr`          // addr to connect to for lease service
	NameAddr           string   `json: nameAddr`           // addr to connect to for nameservice
	DataClientBindAddr string   `json: dataClientBindAddr` // addr we expose for data clients in "x.x.x.x:PORT" syntax.  Must be externally routable as we pass this value to the cluster. 
	NameDataBindAddr   string   `json: nameDataBindAddr`   // addr we expose for nameDataIface, in "x.x.x.x:PORT" syntax. Must be externally routable as we pass this value to the cluster. 
	WebBindAddr        string   `json: webBindAddr`        // addr we expose for web interface, in "x.x.x.x:PORT" syntax. Can be '0.0.0.0'. 
	VolumeRoots        []string `json: volumeRoots`        // list of paths to the roots of the volumes we're exposing
	MountPoint				 string   `json: mountPoint`				// dir to mount to on the client machine, must exist prior to program run
	ConnsPerPeer				 int   `json: connsPerPeer`				// number of connections we hold open in pool per other peer to read/write data
}

func DefaultPeerConfig(bindAddr string, nameHost string, mountPoint string, volRoots []string) *PeerConfig {
	return &PeerConfig{
		fmt.Sprintf("%s:%d", nameHost, DEFAULT_LEASESERVER_PORT),
		fmt.Sprintf("%s:%d", nameHost, DEFAULT_NAMESERVER_PORT),
		fmt.Sprintf("%s:%d", bindAddr, DEFAULT_DATA_PORT),
		fmt.Sprintf("%s:%d", bindAddr, DEFAULT_NAMEDATA_PORT), 
		DEFAULT_DATAWEB_ADDR, // bind to all for web addr
		volRoots,
		mountPoint,
		2,
	}
}

type MasterConfig struct {
	NameBindAddr      string `json: nameBindAddr`      // host:port of namenode
	LeaseBindAddr     string `json: leaseBindAddr`     // host:port for lease service
	WebBindAddr       string `json: webBindAddr`       // host:port for web interface
	NameHome         string `json: nameHome`         // path to nn home on disk
	ReplicationFactor uint32 `json: replicationFactor` // number of replicas for each block
}

func DefaultMasterConfig(nameHome string) *MasterConfig {
	return &MasterConfig{
		DEFAULT_NAMEADDR,
		DEFAULT_LEASEADDR,
		DEFAULT_NAMEWEB_ADDR,
		nameHome,
		3,
	}
}

type FSConfig struct {
	ReplicationFactor uint32 `json: replicationFactor`
	BlockLength uint32 `json: blockLength`
}

type ClientConfig struct {
	LeaseAddr string
	NameAddr  string
}
