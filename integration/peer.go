package integration

import (

	"github.com/jbooth/go-fuse/fuse"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/mrpc"
)

// compile time check for mrpc.Service
var nilPeer mrpc.Service = &Peer{nil,nil,nil}

type Peer struct {
	
	Datanode *dataserver.DataServer
	FuseConnector fuse.RawFileSystem
	svc mrpc.Service
}
func (p *Peer) Serve() error {
	return p.svc.Serve()
}

// requests stop
func (p *Peer)	Close() error {
	return p.svc.Close()
}

// waits till actually stopped
func (p *Peer) WaitClosed() error {
	return p.svc.WaitClosed()
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
		
		ret.svc := mrpc.NewMultiService([]mrpc.Service{Datanode, FuseConnector
		return ret,err
} 

