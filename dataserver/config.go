package dataserver

import (
	"encoding/json"
	"os"
)

type DSConfig struct {
	nameHost           string   // host of namenode
	namePort           int      // main service port on namenode
	dataClientBindAddr string   // addr we expose for data clients in "0.0.0.0:8080" syntax
	nameDataBindAddr   string   // addr we expose for nameDataIface, in "0.0.0.0:8080" syntax
	volumeRoots        []string // list of paths to the roots of the volumes we're exposing 
	datanodeId         int32    // previously configured datanodeID, or null if we're unformatted
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
