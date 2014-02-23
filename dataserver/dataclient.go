package dataserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"math/rand"
	"net"
	"sync"
)

// compile-time check for type safety
var typeCheck maggiefs.DataService = &DataClient{}

type DataClient struct {
	names     maggiefs.NameService
	volMap    map[uint32]*net.TCPAddr
	volLock   *sync.RWMutex // guards volMap
	pool      *connPool
	localDS   *DataServer
	localVols []uint32
}

func NewDataClient(names maggiefs.NameService, connsPerDn int) (*DataClient, error) {
	return &DataClient{names, make(map[uint32]*net.TCPAddr), &sync.RWMutex{}, newConnPool(connsPerDn, true), nil, nil}, nil
}

func pickVol(vols []uint32) uint32 {
	return vols[rand.Int()%len(vols)]
}

func (dc *DataClient) isLocal(vols []uint32) bool {
	if dc.localVols == nil {
		return false
	}
	// both lists are short, double for loop is fine
	for _, volId := range vols {
		for _, localVolId := range dc.localVols {
			if localVolId == volId {
				return true
			}
		}
	}
	return false
}

// get hostname for a volume
func (dc *DataClient) VolHost(volId uint32) (*net.TCPAddr, error) {
	dc.volLock.RLock()
	raddr, exists := dc.volMap[volId]
	dc.volLock.RUnlock()
	if !exists {
		// try refreshing map and try again
		dc.refreshDNs()
		dc.volLock.RLock()
		raddr, exists = dc.volMap[volId]
		dc.volLock.RUnlock()
		if !exists {
			return nil, fmt.Errorf("No dn for vol id %d", volId)
		}
	}
	return raddr, nil
}

// read some bytes
func (dc *DataClient) Read(blk maggiefs.Block, buf maggiefs.SplicerTo, pos uint64, length uint32) (err error) {
	if dc.isLocal(blk.Volumes) {
		// local direct read
		return dc.localDS.DirectRead(blk, buf, pos, length)
	}
	host, err := dc.VolHost(pickVol(blk.Volumes))
	if err != nil {
		return nil
	}

	conn := dc.pool.getConn(host)
	// TODO we could commit the read from the other thread instead of blocking and incurring context switch
	whenDone := make(chan bool, 1)
	header := RequestHeader{OP_READ, blk, pos, length}
	conn.DoRequest(header, nil, func(d *os.File) {

		// read resp header
		resp := &ResponseHeader{}
		_, err = resp.ReadFrom(d)
		if err != nil {
			fmt.Printf("Error reading header from dn : %s", err.Error())
			return
		}
		if resp.Stat == STAT_ERR {
			fmt.Printf("Error code %d from DN", resp.Stat)
			return
		}
		// put header into response buffer
		err = buf.WriteHeader(0, int(length))
		if err != nil {
			fmt.Printf("Error writing resp header to splice pipe : %s", err)
			return
		}
		// read resp bytes
		//fmt.Printf("Entering loop to read %d bytes\n",length)
		numRead := 0
		for uint32(numRead) < length {
			//			fmt.Printf("Reading %d bytes from socket %s into slice [%d:%d]\n", length - uint32(numRead), d, numRead, int(length))
			//			fmt.Printf("Slice length %d capacity %d\n",len(p),cap(p))
			n, err := buf.SpliceBytes(uintptr(d.Rfd()), int(length)-numRead)
			//			fmt.Printf("Read returned %d bytes, first 5: %x\n",n,p[numRead:numRead+5])
			if err != nil {
				fmt.Printf("Error while splicing: %s\n", err)
				return
			}
			numRead += n
		}
		// done
		whenDone <- true
	})
	<-whenDone
	return nil
}

// returns a write session
func (dc *DataClient) WriteSession(blk maggiefs.Block) (writer maggiefs.BlockWriter, err error) {
	//if dc.isLocal(blk.Volumes) {
	//	// local shortcut, return write session from an in-memory pipe
	//	local, remote := PipeEndpoints()
	//	go dc.localDS.serveClientConn(remote)
	//	return newClientPipeline(
	//		local,
	//		blk,
	//		64*1024*1024, // 64MB max for unack'd bytes in flight
	//		func() {
	//			local.Close() // kill in-memory pipe when finished
	//		},
	//	)

	//}
	// return write session over the netwrok

	// find host
	raddr, err := dc.VolHost(blk.Volumes[0])
	if err != nil {
		return nil, err
	}
	// get conn
	conn, err := dc.pool.getConn(raddr)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, err
	}
	return newClientPipeline(
		conn,
		blk,
		64*1024*1024, // 64MB max for unack'd bytes in flight
		func() {
			dc.pool.returnConn(raddr, conn) // return to pool on done
		},
	)
}

func (dc *DataClient) refreshDNs() error {
	dc.volLock.Lock()
	defer dc.volLock.Unlock()
	fsStat, err := dc.names.StatFs()
	if err != nil {
		return err
	}
	// clear existing map
	dc.volMap = make(map[uint32]*net.TCPAddr)
	// fill it up
	for _, dnStat := range fsStat.DnStat {
		dnAddr, err := net.ResolveTCPAddr("tcp", dnStat.Addr)
		if err != nil {
			return err
		}
		for _, volStat := range dnStat.Volumes {
			dc.volMap[volStat.VolId] = dnAddr
		}
	}
	return nil
}

type connPool struct {
	conns map[*net.TCPAddr]*RawClient
	l     *sync.RWMutex
}

func (c *connPool) getConn(host *net.TCPAddr) (*RawClient, error) {
	c.l.RLock()
	ret := c.conns[host]
	c.l.RUnlock()
	if ret != nil {
		return ret, nil
	}
	c.l.Lock()
	conn, err := NewRawClient(host, 16)
	if err == nil {
		c.conns[host] = conn
	}
	c.l.Unlock()
	return conn, err
}
