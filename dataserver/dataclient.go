package dataserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
)

// compile-time check for type safety
var typeCheck maggiefs.DataService = &DataClient{}

type DataClient struct {
	names      maggiefs.NameService
	volMap     map[uint32]*net.TCPAddr
	badVols    map[uint32]bool
	volLock    *sync.RWMutex // guards volMap and badVols
	readConns  *readConnPool
	writeConns *writeConnPool
	localDS    *DataServer
	localVols  []uint32
}

func NewDataClient(names maggiefs.NameService) (*DataClient, error) {
	return &DataClient{
		names,
		make(map[uint32]*net.TCPAddr),
		make(map[uint32]bool),
		&sync.RWMutex{},
		&readConnPool{make(map[*net.TCPAddr]*RawClient), new(sync.RWMutex)},
		&writeConnPool{make(map[*net.TCPAddr]*RawClient), new(sync.RWMutex)},
		nil,
		nil}, nil
}

// get hostname for a volume
// returns nil,nil if this host is currently marked bad.
// returns nil,err if we have never heard of this host
func (dc *DataClient) VolHost(volId uint32) (*net.TCPAddr, error) {
	dc.volLock.RLock()
	isBad, isBadEntryExists := dc.badVols[volId]
	if isBadEntryExists && isBad {
		return nil, nil
	}
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
			delete(dc.badVols, volStat.VolId)
		}
	}
	return nil
}

func (dc *DataClient) markBad(volId uint32) {
	dc.volLock.Lock()
	dc.badVols[volId] = true
	dc.volLock.Unlock()
}

type readCallback struct {
	buf    maggiefs.SplicerTo
	dc     *DataClient
	volId  uint32
	length uint32
	onDone func(error)
}

func (r readCallback) OnResponse(f *os.File) error {
	// read resp bytes
	//fmt.Printf("Entering loop to read %d bytes\n",length)
	//fmt.Printf("In read callback, reading %d bytes", length)
	numRead := 0
	for uint32(numRead) < r.length {
		//fmt.Printf("Reading %d bytes from socket %s into slice [%d:%d]\n", length-uint32(numRead), d, numRead, int(length))
		//      fmt.Printf("Slice length %d capacity %d\n",len(p),cap(p))
		n, err := r.buf.LoadFrom(f.Fd(), int(r.length)-numRead)
		//      fmt.Printf("Read returned %d bytes, first 5: %x\n",n,p[numRead:numRead+5])
		if err != nil {
			log.Printf("Error while splicing: %s\n", err)
			// don't call our onDone -- rawClient will call us back after we return err
			return err
		}
		numRead += n
	}
	r.onDone(nil)
	return nil
}

func (r readCallback) OnErr(err error) {
	r.dc.markBad(r.volId)
	r.onDone(err)
}

// does the read and calls onDone in receiver thread after splicing to SplicerTo
func (dc *DataClient) Read(blk maggiefs.Block, buf maggiefs.SplicerTo, pos uint64, length uint32, onDone func(error)) error {
	// check if this is a local read
	if dc.localVols != nil {
		for _, volId := range blk.Volumes {
			for _, localVolId := range dc.localVols {
				if localVolId == volId {
					return dc.localDS.DirectRead(blk, buf, pos, length, onDone)
				}
			}
		}
	}
	var host *net.TCPAddr = nil
	var err error
	var volId uint32
	for host == nil {
		volId = blk.Volumes[rand.Int()%len(blk.Volumes)]
		host, err = dc.VolHost(volId)
		if err != nil {
			return nil
		}
	}
	//log.Printf("Picked host %s for remote read, getting conn", host)
	conn, err := dc.readConns.getConn(host)
	if err != nil {
		return err
	}
	header := RequestHeader{OP_READ, 0, blk, pos, length}
	rcb := readCallback{buf, dc, volId, length, onDone}
	return conn.DoRequest(header, nil, rcb)
}

type writeCallback struct {
	dc     *DataClient
	volId  uint32
	onDone func(error)
}

func (w writeCallback) OnResponse(f *os.File) error {
	w.onDone(nil)
	return nil
}

func (w writeCallback) OnErr(err error) {
	w.dc.markBad(w.volId)
	w.onDone(err)
}

func (dc *DataClient) Write(blk maggiefs.Block, p []byte, pos uint64, onDone func(error)) (err error) {
	// always go in order of volumes on block
	host, err := dc.VolHost(blk.Volumes[0])
	if err != nil {
		return err
	}
	conn, err := dc.writeConns.getConn(host)
	if err != nil {
		return err
	}
	reqHeader := RequestHeader{OP_WRITE, 0, blk, pos, uint32(len(p))}
	cb := writeCallback{dc, blk.Volumes[0], onDone}
	return conn.DoRequest(reqHeader, p, cb)
}

type connPool struct {
	conns map[*net.TCPAddr]*RawClient
	l     *sync.RWMutex
}

type readConnPool connPool
type writeConnPool connPool

func (c *readConnPool) getConn(host *net.TCPAddr) (*RawClient, error) {
	c.l.RLock()
	ret := c.conns[host]
	c.l.RUnlock()
	if ret != nil {
		return ret, nil
	}
	c.l.Lock()
	conn, err := mrpc.DialHandler(host, DIAL_READ)
	if err != nil {
		return nil, err
	}
	cli, err := NewRawClient(conn, 16)
	if err == nil {
		c.conns[host] = cli
	}
	c.l.Unlock()
	return cli, err
}

func (c *writeConnPool) getConn(host *net.TCPAddr) (*RawClient, error) {
	c.l.RLock()
	ret := c.conns[host]
	c.l.RUnlock()
	if ret != nil {
		return ret, nil
	}
	c.l.Lock()
	conn, err := mrpc.DialHandler(host, DIAL_WRITE)
	if err != nil {
		return nil, err
	}
	cli, err := NewRawClient(conn, 16)
	if err == nil {
		c.conns[host] = cli
	}
	c.l.Unlock()
	return cli, err
}
