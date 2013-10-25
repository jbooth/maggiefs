package dataserver

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"net"
	"sync"
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
	nameDataAddr string
	// dataservice for pipelining writes
	dc *DataClient
	// locks to manage shutdown
	clos *sync.Cond
	closed bool
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
	fp := NewFilePool(256,128)
	for _, volRoot := range volRoots {
		if validVolume(volRoot) {
			// initialize existing volume
			vol, err := loadVolume(volRoot,fp)
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
		// assign if we haven't assigned yet
		if dnInfo.DnId == 0 {
			dnInfo = vol.info.DnInfo
		} else {
			// if we already assigned, compare new to previous and make sure we're not inconsistent
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
		vol, err := formatVolume(path, maggiefs.VolumeInfo{volId, dnInfo},fp)
		if err != nil {
			return nil, err
		}
		volumes[volId] = vol
	}

	// bind to listener sockets
	dataClientBind, err := net.ResolveTCPAddr("tcp", dataClientBindAddr)
	if err != nil {
		return nil, err
	}

	dataClientListen, err := net.ListenTCP("tcp", dataClientBind)
	if err != nil {
		return nil, err
	}
	ds = &DataServer{ns, dnInfo, volumes, dataClientListen, nil, nameDataBindAddr, dc, sync.NewCond(new(sync.Mutex)),false}


	ds.nameDataIface, err = mrpc.CloseableRPC(nameDataBindAddr, mrpc.NewNameDataIfaceService(ds), "NameDataIface")
	if err != nil {
		return ds, err
	}
	return ds, nil
}

func (ds *DataServer) Serve() error {
	errChan := make(chan error,3)
	go func() {
		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic from nameserver rpc: %v\n", x)
				errChan <- fmt.Errorf("Run time panic: %v", x)
			}
		}()
		errChan <- ds.nameDataIface.Serve()
	}()

	go func() {
		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic from nameserver web: %v\n", x)
				errChan <- fmt.Errorf("Run time panic: %v", x)
			}
		}()
		errChan <- ds.serveClientData()
	}()
	// tell namenode we're joining cluster
	err := ds.ns.Join(ds.info.DnId, ds.nameDataAddr)
	if err != nil {
		ds.Close()
		return err
	}
	err = <- errChan
	return err
}

func (ds *DataServer) Close() error {
	ds.clos.L.Lock()
	defer ds.clos.L.Unlock()
	if ds.closed { 
		return nil
	}
	ds.nameDataIface.Close()
	ds.dataIface.Close()
	for _, v := range ds.volumes {
		v.Close()
	}
	ds.closed = true
	ds.clos.Broadcast()
	return nil
}

func (ds *DataServer) WaitClosed() error {
	ds.clos.L.Lock()
	for ! ds.closed {
		ds.clos.Wait()
	}
	ds.clos.L.Unlock()
	return ds.nameDataIface.WaitClosed()
}

func (ds *DataServer) serveClientData() error {
	for {
		tcpConn, err := ds.dataIface.AcceptTCP()
		if err != nil {
			fmt.Printf("Error accepting client on listen addr, shutting down: %s\n", err.Error())
			return err
		}
		tcpConn.SetNoDelay(true)
		go ds.serveClientConn(SockEndpoint(tcpConn))
	}
}

func (ds *DataServer) serveClientConn(conn Endpoint) {
	defer conn.Close()
	for {
		req := RequestHeader{}
		_, err := req.ReadFrom(conn)
		if err != nil {
			fmt.Printf("Err serving conn %s : %s\n", conn.String(), err.Error())
			return
		}
		// figure out which of our volumes
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
			_, err := resp.WriteTo(conn)
			if err != nil {
				fmt.Printf("Err serving conn %s : %s", conn.String(), err.Error())
				return
			}
		} else {
			vol := ds.volumes[volForBlock]
			if req.Op == OP_READ {
				err = vol.serveRead(conn, req)
				if err != nil {
					fmt.Printf("Err serving conn %s : %s", conn.String(), err.Error())
					return
				}
			} else if req.Op == OP_WRITE {
				err = vol.serveWrite(conn, req, ds.dc)
				if err != nil {
					fmt.Printf("Err serving conn %s : %s", conn.String(), err.Error())
					return
				}
			} else {
				// unrecognized req, send err response
				resp := &ResponseHeader{STAT_BADOP}
				_, err := resp.WriteTo(conn)
				if err != nil {
					fmt.Printf("Err serving conn %s : %s", conn.String(), err.Error())
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
	vol, exists := ds.volumes[volId]
	if !exists {
		fmt.Printf("No volume for volID %d on dnId %d \n", volId, ds.info.DnId)
		return nil, fmt.Errorf("No volume for volID %d", volId)
	}
	return vol.BlockReport()
}

// read/write methods
