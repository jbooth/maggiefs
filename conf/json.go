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


func (cfg *NSConfig) Write(out io.Writer) error {
  return json.NewEncoder(out).Encode(cfg)
}

func (cfg *NSConfig) Writef(file string) error {
  f, err := os.Create(file)
  if err != nil {
    return err
  }
  defer f.Close()
  return cfg.Write(f)
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

func (ds *DSConfig) Write(out io.Writer) error {
	return json.NewEncoder(f).Encode(ds)
}

func (ds *DSConfig) Writef(file string) error {
  f, err := os.Create(file)
  if err != nil {
    return err
  }
  defer f.Close()
  return ds.Write(f)
}