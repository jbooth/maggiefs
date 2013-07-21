package dataserver

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"net"
)

type DataServer struct {
	ns   maggiefs.NameService
	info maggiefs.DataNodeInfo
	// live and unformatted volumes
	volumes map[uint32]*volume
	// accepts data conns for read/write requests
	dataIface *net.TCPListener
	// accepts conn from namenode
	nameDataIface *mrpc.CloseableServer
	// dataservice for pipelining writes
	dc *DataClient
}

// create a new dataserver serving the specified volumes, on the specified addrs, joining the specified nameservice
func NewDataServer(volRoots []string,
	dataClientBindAddr string,
	nameDataBindAddr string,
	webBindAddr string,
	ns maggiefs.NameService,
	dc *DataClient) (ds *DataServer, err error) {
	// scan volumes
	volumes := make(map[uint32]*volume)
	unformatted := make([]string, 0)
	for _, volRoot := range volRoots {
		if validVolume(volRoot) {
			// initialize existing volume
			vol, err := loadVolume(volRoot)
			if err != nil {
				return nil, err
			}
			volumes[vol.id] = vol
		} else {
			// mark unformatted, wait for order from namenode to format
			unformatted = append(unformatted, volRoot)
		}
	}
	// form consensus on host across volumes or error
	var dnInfo maggiefs.DataNodeInfo = maggiefs.DataNodeInfo{}
	for _, vol := range volumes {
		if dnInfo.DnId != 0 {
			dnInfo = vol.info.DnInfo
		} else {
			// compare new to previous
			if !dnInfo.Equals(vol.info.DnInfo) {
				return nil, errors.New("Incompatible dataNodeInfo across volumes!")
			}
		}
	}
	if dnInfo.DnId == 0 {
		dnInfo.DnId, err = ns.NextDnId()
	}
	dnInfo.Addr = dataClientBindAddr

	// format unformatted volumes
	for _, path := range unformatted {
		volId, err := ns.NextVolId()
		if err != nil {
			return nil, err
		}
		vol, err := formatVolume(path, maggiefs.VolumeInfo{volId, dnInfo})
		if err != nil {
			return nil, err
		}
		volumes[volId] = vol
	}

	// start up listeners
	dataClientBind, err := net.ResolveTCPAddr("tcp", dataClientBindAddr)
	if err != nil {
		return nil, err
	}

	dataClientListen, err := net.ListenTCP("tcp", dataClientBind)
	if err != nil {
		return nil, err
	}

	// start servicing namedata
	ds = &DataServer{ns, dnInfo, volumes, dataClientListen, nil, dc}

	ds.nameDataIface, err = mrpc.CloseableRPC(nameDataBindAddr, mrpc.NewNameDataIfaceService(ds), "NameDataIface")
	if err != nil {
		return ds, err
	}
	ds.nameDataIface.Start()
	// start servicing client data
	go ds.serveClientData()
	// register ourselves with namenode, namenode will query us for volumes
	err = ns.Join(dnInfo.DnId, nameDataBindAddr)
	return ds, nil
}

func (ds *DataServer) Start() error {
	err1 := ds.nameDataIface.Start()
	if err1 != nil {
		return err1
	}
	go ds.serveClientData()
	return nil
}

func (ds *DataServer) Close() {

	ds.nameDataIface.Close()
	ds.dataIface.Close()
	for _, v := range ds.volumes {
		v.Close()
	}
}

func (ds *DataServer) WaitClosed() error {
	return ds.nameDataIface.WaitClosed()
}

func (ds *DataServer) serveClientData() {
	for {
		tcpConn, err := ds.dataIface.AcceptTCP()
		if err != nil {
			fmt.Printf("Error accepting client on listen addr, shutting down: %s\n", err.Error())
			return
		}
		tcpConn.SetNoDelay(true)
		fmt.Printf("Accepted new conn to DS at addr %s\n",ds.dataIface.Addr().String())
		connFile,err := newConnFile(tcpConn)
		if err != nil {
		  fmt.Printf("Error getting file from connected client %s: %s shutting down\n",tcpConn.RemoteAddr().String(),err.Error())
		  return
		}
		go ds.serveClientConn(connFile)
	}
}



func (ds *DataServer) serveClientConn(conn *connFile) {
	defer conn.f.Close()
	for {
		req := RequestHeader{}
		_, err := req.ReadFrom(conn.f)
		if err != nil {
			fmt.Printf("Err serving conn %s : %s", conn.RemoteAddr, err.Error())
			return
		}
		// figure out which if our volumes
		volForBlock := uint32(0)
		for volId, _ := range ds.volumes {
			for _, blockVolId := range req.Blk.Volumes {
				if blockVolId == volId {
					volForBlock = blockVolId
				}
			}
		}
		if volForBlock == 0 {
			// return error and reloop
			resp := &ResponseHeader{STAT_BADVOLUME}
			_, err := resp.WriteTo(conn.f)
			if err != nil {
				fmt.Printf("Err serving conn %s : %s", conn.RemoteAddr, err.Error())
				return
			}
		} else {
			vol := ds.volumes[volForBlock]
			if req.Op == OP_READ {
				fmt.Println("serving read")
				err = vol.serveRead(conn.f, req)
				if err != nil {
					fmt.Printf("Err serving conn %s : %s", conn.RemoteAddr, err.Error())
					return
				}
			} else if req.Op == OP_WRITE {
				err = vol.serveWrite(conn.f, req,ds.dc)
				if err != nil {
					fmt.Printf("Err serving conn %s : %s", conn.RemoteAddr, err.Error())
					return
				}
			} else {
				// unrecognized req, send err response
				resp := &ResponseHeader{STAT_BADOP}
				_, err := resp.WriteTo(conn.f)
				if err != nil {
					fmt.Printf("Err serving conn %s : %s", conn.RemoteAddr, err.Error())
					return
				}
			}
		}

	}
}

func (ds *DataServer) HeartBeat() (stat *maggiefs.DataNodeStat, err error) {
	ret := maggiefs.DataNodeStat{ds.info, make([]maggiefs.VolumeStat, len(ds.volumes), len(ds.volumes))}
	idx := 0
	for _, vol := range ds.volumes {
		ret.Volumes[idx], err = vol.HeartBeat()
		if err != nil {
			return nil, err
		}
		idx++
	}
	return &ret, nil
}

func (ds *DataServer) AddBlock(blk maggiefs.Block, volId uint32) (err error) {
	vol, exists := ds.volumes[volId]
	if !exists {
		return fmt.Errorf("No volume for volID %d", volId)
	}
	return vol.AddBlock(blk)
}

func (ds *DataServer) RmBlock(id uint64, volId uint32) (err error) {
	vol, exists := ds.volumes[volId]
	if !exists {
		return fmt.Errorf("No volume for volID %d", volId)
	}
	return vol.RmBlock(id)
}

func (ds *DataServer) TruncBlock(blk maggiefs.Block, volId uint32, newSize uint32) (err error) {
	vol, exists := ds.volumes[volId]
	if !exists {
		return fmt.Errorf("No volume for volID %d", volId)
	}
	return vol.TruncBlock(blk, newSize)
}

func (ds *DataServer) BlockReport(volId uint32) (blocks []maggiefs.Block, err error) {
	fmt.Println("doing block report\n")
	vol, exists := ds.volumes[volId]
	if !exists {
		fmt.Printf("No volume for volID %d on dnId %d \n", volId, ds.info.DnId)
		return nil, fmt.Errorf("No volume for volID %d", volId)
	}
	fmt.Printf("delegating to volid %d\n", vol.id)
	return vol.BlockReport()
}

// read/write methods
