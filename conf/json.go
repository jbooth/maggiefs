package conf

import (
	"encoding/json"
	"io"
	"os"
)

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
