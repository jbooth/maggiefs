package conf

import (
  "os"
  "encoding/json"
)


func (cfg *NSConfig) ReadConfig(file string) error {
  f, err := os.Open(file)
  if err != nil {
    return err
  }
  defer f.Close()
  d := json.NewDecoder(f)
  return d.Decode(cfg)
}

func (cfg *NSConfig) Write(file string) error {
  f, err := os.Create(file)
  if err != nil {
    return err
  }
  defer f.Close()
  err = json.NewEncoder(f).Encode(cfg)
  return err
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