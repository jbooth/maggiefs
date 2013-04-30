package integration

import (
  "encoding/json"
  "os"
)

type DSConfig struct {
  NameAddr           string   // addr to connect to for nameservice
  DataClientBindAddr string   // addr we expose for data clients in "0.0.0.0:PORT" syntax
  NameDataBindAddr   string   // addr we expose for nameDataIface, in "0.0.0.0:PORT" syntax
  VolumeRoots        []string // list of paths to the roots of the volumes we're exposing 
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


type NNConfig struct {
  NameBindAddr           string   // host:port of namenode
  LeaseBindAddr          string   // host:port for lease service
  NNHomeDir              string   // path to nn home on disk
}


func (cfg *NNConfig) ReadConfig(file string) (error) {
  f, err := os.Open(file)
  if err != nil {
    return err
  }
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