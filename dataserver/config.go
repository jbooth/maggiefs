package dataserver

import (
  "os"
  "encoding/json"
)

type DSConfig struct {
  nameHost string  // host of namenode
  namePort     int // main service port on namenode
  nameDataPort int // port of namenode for datanodes to connect to
  dataPort int     // port we're exposing for dataservice
  volumeRoots []string // list of paths to the roots of the volumes we're exposing 
}

func ReadConfig(file string) {
  
}

func (ds *DSConfig) Write(file string) error {
  f,err := os.Create(file)
  if err != nil { return err }
  defer f.Close()
  err = json.NewEncoder(f).Encode(ds)
  return err
}