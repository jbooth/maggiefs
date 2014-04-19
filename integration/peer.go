package integration

import (
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/dataserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"net"
)

// compile time check for Service
var peerTypeCheck Service = &Peer{}

type Peer struct {
	Cfg        *PeerConfig
	Mfs        *client.MaggieFuse
	Datanode   *dataserver.DataServer
	Mountpoint *Mount
	svc        Service
}

func (p *Peer) Serve() error {
	return p.svc.Serve()
}

// requests stop
func (p *Peer) Close() error {
	return p.svc.Close()
}

// waits till actually stopped
func (p *Peer) WaitClosed() error {
	return p.svc.WaitClosed()
}

func NewPeer(cfg *PeerConfig, debug bool) (*Peer, error) {

	cl, err := NewClient(cfg.MasterAddr)
	if err != nil {
		return nil, err
	}
	ret := &Peer{}
	ret.Cfg = cfg
	ret.Datanode, err = dataserver.NewDataServer(cfg.BindAddr, cfg.VolumeRoots, cl.Names, cl.Datas)
	if err != nil {
		return ret, err
	}
	dnStat, err := ret.Datanode.HeartBeat()
	if err != nil {
		return ret, err
	}
	fuseConnector, err := client.NewMaggieFuse(cl.Leases, cl.Names, cl.Datas, &dnStat.DnId)
	if err != nil {
		return ret, err
	}
	ret.Mfs = fuseConnector
	ret.Mountpoint, err = NewMount(fuseConnector, cfg.MountPoint, false)

	opMap := make(map[uint32]func(*net.TCPConn))
	opMap[dataserver.DIAL_READ] = ret.Datanode.ServeReadConn
	opMap[dataserver.DIAL_WRITE] = ret.Datanode.ServeWriteConn

	dataServ, err := mrpc.CloseableRPC(cfg.BindAddr, "Peer", maggiefs.NewPeerService(ret.Datanode), opMap)
	if err != nil {
		return ret, err
	}
	multiServ := NewMultiService()

	err = multiServ.AddService(dataServ)
	if err != nil {
		return ret, err
	}
	err = multiServ.AddService(ret.Mountpoint)
	if err != nil {
		return ret, err
	}
	// now that services are started, tell master to connect to us
	err = cl.Names.Join(dnStat.DnId, cfg.BindAddr)

	ret.svc = multiServ
	return ret, err
}
