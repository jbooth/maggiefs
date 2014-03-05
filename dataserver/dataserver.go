package dataserver

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"io"
	"net"
	"os"
	"sync"
	"syscall"
)

type DataServer struct {
	ns   maggiefs.NameService
	info maggiefs.DataNodeInfo
	// live and unformatted volumes
	volumes map[uint32]*volume
	// accepts data conns for read/write requests
	dataClientAddr *net.TCPAddr
	dataIface      *net.TCPListener
	// accepts conn from namenode
	nameDataIface *mrpc.CloseableServer
	nameDataAddr  string
	// dataservice for pipelining writes
	dc maggiefs.DataService
	// locks to manage shutdown
	clos   *sync.Cond
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
	fp := NewFilePool(256, 128)
	for _, volRoot := range volRoots {
		if validVolume(volRoot) {
			// initialize existing volume
			vol, err := loadVolume(volRoot, fp)
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
		vol, err := formatVolume(path, maggiefs.VolumeInfo{volId, dnInfo}, fp)
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
	ds = &DataServer{ns, dnInfo, volumes, dataClientBind, dataClientListen, nil, nameDataBindAddr, dc, sync.NewCond(new(sync.Mutex)), false}

	ds.nameDataIface, err = mrpc.CloseableRPC(nameDataBindAddr, mrpc.NewNameDataIfaceService(ds), "NameDataIface")
	if err != nil {
		return ds, err
	}
	return ds, nil
}

func (ds *DataServer) Serve() error {
	errChan := make(chan error, 3)
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
	err = <-errChan
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
	for !ds.closed {
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
		tcpConn.SetReadBuffer(1024 * 1024)
		tcpConn.SetWriteBuffer(1024 * 1024)
		f, err := tcpConn.File()
		if err != nil {
			return err
		}
		err = syscall.SetNonblock(int(f.Fd()), false)
		if err != nil {
			return err
		}
		go ds.serveClientConn(f)
	}
}

func (ds *DataServer) serveClientConn(conn *os.File) {
	defer conn.Close()
	buff := make([]byte, 128*1024, 128*1024)
	l := new(sync.Mutex)
	for {
		req := &RequestHeader{}
		_, err := req.ReadFrom(conn)

		if err != nil {
			// don't log error for remote closed connection
			if err != io.EOF && err != io.ErrClosedPipe {
				fmt.Printf("Err serving conn while reading header %s : %s\n", "conn", err.Error())
			}
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
			resp := &ResponseHeader{STAT_BADVOLUME, req.Reqno}
			_, err := resp.WriteTo(conn)
			if err != nil {
				fmt.Printf("Err serving conn %s : %s", "tcpconn", err.Error())
				return
			}
		} else {
			vol := ds.volumes[volForBlock]
			if req.Op == OP_READ {
				l.Lock()
				err = vol.serveRead(conn, req)
				l.Unlock()
				if err != nil {
					fmt.Printf("Err serving conn %s : %s", "tcpconn", err.Error())
					return
				}
			} else if req.Op == OP_WRITE {
				writeBuff := buff[:int(req.Length)]
				_, err := conn.Read(writeBuff)
				if err != nil {
					fmt.Printf("Err serving conn %s : %s", "tcpconn", err.Error())
					return
				}
				insureWriteFinished := make(chan bool, 1)
				resp := ResponseHeader{STAT_OK, req.Reqno}
				if len(req.Blk.Volumes) > 1 {
					// forward to next node if appropriate with callback
					req.Blk.Volumes = req.Blk.Volumes[1:]
					ds.dc.Write(req.Blk, writeBuff, req.Pos, func() {
						// wait for local write to complete (should be done before we hear back from remote anyways)
						<-insureWriteFinished
						// send response to client
						l.Lock()
						fmt.Printf("Intermediate note writing response %+v\n", resp)
						_, err := resp.WriteTo(conn)
						l.Unlock()
						if err != nil {
							fmt.Printf("Error sending response to client %s\n", err)
						}
					})
				}
				// write to our copy of block
				err = vol.withFile(req.Blk.Id, func(f *os.File) error {
					_, e1 := f.WriteAt(writeBuff, int64(req.Pos))
					return e1
				})
				if err != nil {
					fmt.Printf("Err writing to file: %s\n")
					return
				}
				if len(req.Blk.Volumes) == 1 {
					// respond here
					l.Lock()
					fmt.Printf("Terminal node writing response %+v\n", resp)
					_, err := resp.WriteTo(conn)
					l.Unlock()
					if err != nil {
						fmt.Printf("Error sending response to client %s\n", err)
					}
				} else {
					// tell the responder thread it's ok to send
					insureWriteFinished <- true
				}

			} else {
				// unrecognized req, send err response
				resp := &ResponseHeader{STAT_BADOP, req.Reqno}
				_, err := resp.WriteTo(conn)
				if err != nil {
					fmt.Printf("Err serving conn %s : %s", "conn", err.Error())
					return
				}
			}
		}

	}
}

func (ds *DataServer) DirectRead(blk maggiefs.Block, buf maggiefs.SplicerTo, pos uint64, length uint32) (err error) {
	req := &RequestHeader{OP_READ, 0, blk, pos, length}
	// figure out which of our volumes
	volForBlock := uint32(0)
	var volWithBlock *volume = nil
	for volId, vol := range ds.volumes {
		for _, blockVolId := range req.Blk.Volumes {
			if blockVolId == volId {
				volForBlock = blockVolId
				volWithBlock = vol
			}
		}
	}
	if volForBlock == 0 {
		// return error and reloop
		return fmt.Errorf("No valid volume for block %+v", blk)
	}
	return volWithBlock.serveDirectRead(buf, req)
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

func (ds *DataServer) AddBlock(blk maggiefs.Block, volId uint32, fallocate bool) (err error) {
	vol, exists := ds.volumes[volId]
	if !exists {
		return fmt.Errorf("No volume for volID %d", volId)
	}
	return vol.AddBlock(blk, fallocate)
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
