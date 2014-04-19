package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

var (
	DEFAULT_NAMEBIND = fmt.Sprintf("0.0.0.0:%d", 1103)
)

// config for a peer -- connects to MasterAddr and binds to BindAddr
// note, BindAddr should be a remote-accessible address -- don't use 0.0.0.0!
type PeerConfig struct {
	MasterAddr  string   `json: masterAddr`  // addr to connect to for master service, should be in host:PORT syntax
	BindAddr    string   `json: bindAddr`    // addr we expose for our services in "x.x.x.x:PORT" syntax, needs to be externally routable, can NOT be 0.0.0.0,
	VolumeRoots []string `json: volumeRoots` // list of paths to the roots of the volumes we're exposing
	MountPoint  string   `json: mountPoint`  // dir to mount to on the client machine, must exist prior to program run
}

type MasterConfig struct {
	BindAddr          string `json: bindAddr`          // host:port for nameservice and leaseservice
	NameHome          string `json: nameHome`          // path to nn home on disk
	ReplicationFactor uint32 `json: replicationFactor` // number of replicas for each block
}

// defaults
func DefaultPeerConfig(bindAddr string, nameHost string, mountPoint string, volRoots []string) *PeerConfig {
	return &PeerConfig{
		fmt.Sprintf("%s:%d", nameHost, 1103),
		bindAddr,
		volRoots,
		mountPoint,
	}
}

func DefaultMasterConfig(nameHome string) *MasterConfig {
	return &MasterConfig{
		DEFAULT_NAMEBIND,
		nameHome,
		3,
	}
}

// serialization methods
func (cfg *MasterConfig) ReadConfig(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	return d.Decode(cfg)
}

func (cfg *MasterConfig) Write(out io.Writer) error {
	return json.NewEncoder(out).Encode(cfg)
}

func (cfg *MasterConfig) Writef(file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	return cfg.Write(f)
}

func (ds *PeerConfig) ReadConfig(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	return d.Decode(ds)
}

func (ds *PeerConfig) Write(out io.Writer) error {
	return json.NewEncoder(out).Encode(ds)
}

func (ds *PeerConfig) Writef(file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	return ds.Write(f)
}
