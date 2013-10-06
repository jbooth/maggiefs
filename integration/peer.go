package integration

import (

	"github.com/jbooth/go-fuse/fuse"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/mrpc"
)

// compile time check for mrpc.Service
var nilPeer mrpc.Service = &Peer{nil,nil}

type Peer struct {
	Datanode *dataserver.DataServer
	FuseConnector fuse.RawFileSystem
}
func (p *Peer)   
// requests stop
func (p *Peer)	Close() error {
	p.Datanode.Close()
	return nil
}

// waits till actually stopped
func (p *Peer) WaitClosed() error {
	return p.Datanode.WaitClosed()
}

func NewPeer(cfg *conf.PeerConfig) (*Peer, error) {
		
		cl,err :=  NewClient(cfg)
		if err != nil {
			return nil,err
		}
		ret := &Peer{}
		ret.Datanode, err = dataserver.NewDataServer(cfg.VolumeRoots, cfg.DataClientBindAddr, cfg.NameDataBindAddr, cfg.WebBindAddr, cl.Names, cl.Datas)
		if err != nil {
			return ret,err
		}
		ret.FuseConnector,err = client.NewMaggieFuse(cl.Leases,cl.Names,cl.Datas)
		return ret,err
} 

