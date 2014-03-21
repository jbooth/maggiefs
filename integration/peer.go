package integration

import (
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/conf"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/mrpc"
)

// compile time check for mrpc.Service
var peerTypeCheck mrpc.Service = &Peer{}

type Peer struct {
	Cfg 	 *conf.PeerConfig
	Mfs      *client.MaggieFuse
	Datanode *dataserver.DataServer
	Mountpoint *Mount
	Web  *PeerWebServer
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

func (p *Peer) HttpAddr() string {
	return p.Cfg.WebBindAddr
}

func NewPeer(cfg *conf.PeerConfig, debug bool) (*Peer, error) {
		
		cl,err :=  NewClient(cfg)
		if err != nil {
			return nil,err
		}
		ret := &Peer{}
		ret.Cfg = cfg
		ret.Datanode, err = dataserver.NewDataServer(cfg.VolumeRoots, cfg.DataClientBindAddr, cfg.NameDataBindAddr, cfg.WebBindAddr, cl.Names, cl.Datas)
		if err != nil {
			return ret,err
		}
		dnStat,err := ret.Datanode.HeartBeat()
		if err != nil {
			return ret,err
		}
		fuseConnector,err := client.NewMaggieFuse(cl.Leases,cl.Names,cl.Datas, &dnStat.DnId)
		if err != nil {
			return ret,err
		}	
		ret.Mfs = fuseConnector
		ret.Mountpoint,err = NewMount(fuseConnector,cfg.MountPoint,false)

		opMap := make(map[uint32]func(*net.TCPConn))
		opMap[dataserver.DIAL_READ] = ret.Datanode.ServeReadConn
		opMap[dataserver.DIAL_WRITE] = ret.Datanode.ServeWriteConn
	
		dataServ,err := mrpc.CloseableRPC(cfg.BindAddr, impl interface{}, customHandlers map[uint32]func(newlyAcceptedConn *net.TCPConn), name string) (*CloseableServer, error) {
		if err != nil {
			return ret,err
		}
		multiServ := NewMultiService()

		err = multiServ.AddService(dataServ)
		if err != nil {
			return ret,err
		}
		ret.Web,err = NewPeerWebServer(cl.Names, cl.Datas, cfg.MountPoint, cfg.WebBindAddr)
		if err != nil {
			return ret,err
		}
		err = multiServ.AddService(ret.Web)
		if err != nil {
			return ret,err
		}
		err = multiServ.AddService(ret.Mountpoint)
		if err != nil {
			return ret,err
		}
		ret.svc = multiServ
		return ret,err
} 

