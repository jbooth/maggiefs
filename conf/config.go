package conf

import (
	"encoding/json"
	"os"
)

type DSConfig struct {
	LeaseAddr          string   `json: leaseAddr`          // addr to connect to for lease service
	NameAddr           string   `json: nameAddr`           // addr to connect to for nameservice
	DataClientBindAddr string   `json: dataClientBindAddr` // addr we expose for data clients in "0.0.0.0:PORT" syntax
	NameDataBindAddr   string   `json: nameDataBindAddr`   // addr we expose for nameDataIface, in "0.0.0.0:PORT" syntax
	WebBindAddr        string   `json: webBindAddr`        // addr we expose for web interface, in "0.0.0.0:PORT" syntax
	VolumeRoots        []string `json: volumeRoots`        // list of paths to the roots of the volumes we're exposing
}

func (ds *DSConfig) ReadConfig(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	return d.Decode(ds)
}

func (ds *DSConfig) Write(file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	err = json.NewEncoder(f).Encode(ds)
	return err
}

type NNConfig struct {
	NameBindAddr      string `json: nameBindAddr`      // host:port of namenode
	LeaseBindAddr     string `json: leaseBindAddr`     // host:port for lease service
	WebBindAddr       string `json: webBindAddr`       // host:port for web interface
	NNHomeDir         string `json: nnHomeDir`         // path to nn home on disk
	ReplicationFactor uint32 `json: replicationFactor` // number of replicas for each block
}

type FSConfig struct {
	BlockLength uint32 `json: blockLength`
}

func (cfg *NNConfig) ReadConfig(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	return d.Decode(cfg)
}

func (cfg *NNConfig) Write(file string) error {
	f, err := os.Create(file)
	if err != nil {
		return err
	}
	defer f.Close()
	err = json.NewEncoder(f).Encode(cfg)
	return err
}

type ClientConfig struct {
	LeaseAddr string
	NameAddr  string
}
