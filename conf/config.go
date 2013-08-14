package integration

import (
  "encoding/json"
  "os"
)

type DSConfig struct {
	LeaseAddr					 string   // addr to connect to for lease service
  NameAddr           string   // addr to connect to for nameservice
  DataClientBindAddr string   // addr we expose for data clients in "0.0.0.0:PORT" syntax
  NameDataBindAddr   string   // addr we expose for nameDataIface, in "0.0.0.0:PORT" syntax
  WebBindAddr        string   // addr we expose for web interface, in "0.0.0.0:PORT" syntax
  VolumeRoots        []string // list of paths to the roots of the volumes we're exposing 
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
  NameBindAddr           string   // host:port of namenode
  LeaseBindAddr          string   // host:port for lease service
  WebBindAddr        string   // host:port for web interface
  NNHomeDir              string   // path to nn home on disk
  ReplicationFactor			 uint32   // number of replicas for each block
}


func (cfg *NNConfig) ReadConfig(file string) (error) {
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
  NameAddr string
}