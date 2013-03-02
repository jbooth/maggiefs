package dataserver

import (
	"encoding/json"
	"os"
)

type DSConfig struct {
	NameHost           string   // host of namenode
	NamePort           int      // main service port on namenode
	DataClientBindAddr string   // addr we expose for data clients in "0.0.0.0:8080" syntax
	NameDataBindAddr   string   // addr we expose for nameDataIface, in "0.0.0.0:8080" syntax
	VolumeRoots        []string // list of paths to the roots of the volumes we're exposing 
	DatanodeId         int32    // previously configured datanodeID, or null if we're unformatted
	DnHome             string   // DN homedir on a locally mounted disk
}

func ReadConfig(file string) (*DSConfig, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	ret := &DSConfig{}
	d := json.NewDecoder(f)
	d.Decode(ret)
	return ret, nil
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
