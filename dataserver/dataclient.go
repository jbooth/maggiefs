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
	names   maggiefs.NameService
	volMap  map[uint32]*net.TCPAddr
	volLock *sync.RWMutex // guards volMap
	pool    *connPool
	localDS *DataServer
	localVols []uint32
}

func NewDataClient(names maggiefs.NameService, connsPerDn int) (*DataClient, error) {
	return &DataClient{names, make(map[uint32]*net.TCPAddr), &sync.RWMutex{}, newConnPool(connsPerDn, true),nil,nil}, nil
}

func pickVol(vols []uint32) uint32 {
	return vols[rand.Int()%len(vols)]
}

func (dc *DataClient) isLocal(vols []uint32) bool {
	if dc.localVols == nil {
		return false
	}
	// both lists are short, double for loop is fine
	for _,volId := range vols {
		for _,localVolId := range dc.localVols {
			if localVolId == volId { return true }
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
	if (dc.isLocal(blk.Volumes)) {
		// local direct read
		return dc.localDS.DirectRead(blk,buf,pos,length)
	}
	return dc.withConn(pickVol(blk.Volumes), func(d Endpoint) error {
		// send req
		header := RequestHeader{OP_READ, blk, pos, length}
		//		fmt.Printf("data client writing read header to %s\n", d)
		//		fmt.Printf("Reading length %d to slice of length %d\n", length, len(p))
		fmt.Printf("Sending read header in dataclient : %+v \n",header)
		_, err = header.WriteTo(d)
		if err != nil {
			return fmt.Errorf("Error writing header to dn : %s", err.Error())
		}
		// read resp header
		resp := &ResponseHeader{}
		_, err = resp.ReadFrom(d)
		if err != nil {
			return fmt.Errorf("Error reading header from dn : %s", err.Error())
		}
		if resp.Stat == STAT_ERR {
			return fmt.Errorf("Error code %d from DN", resp.Stat)
		}
		// put header into response buffer
		err = buf.WriteHeader(0,int(length))
		if err != nil {
			return fmt.Errorf("Error writing resp header to splice pipe : %s",err)
		}
		
		
		// read resp bytes
		//fmt.Printf("Entering loop to read %d bytes\n",length)
		numRead := 0
		for uint32(numRead) < length {
			//			fmt.Printf("Reading %d bytes from socket %s into slice [%d:%d]\n", length - uint32(numRead), d, numRead, int(length))
			//			fmt.Printf("Slice length %d capacity %d\n",len(p),cap(p))
			n, err := buf.SpliceBytes(uintptr(d.Rfd()),int(length) - numRead)
			//			fmt.Printf("Read returned %d bytes, first 5: %x\n",n,p[numRead:numRead+5])
			if err != nil {
				return err
			}
			numRead += n
		}
		return nil
	})
}

// returns a write session
func (dc *DataClient) WriteSession(blk maggiefs.Block) (writer maggiefs.BlockWriter, err error) {
	fmt.Printf("Fetching write session in dataclient \n")
	if dc.isLocal(blk.Volumes) {
		// local shortcut, return write session from an in-memory pipe
		local,remote := PipeEndpoints()
		go dc.localDS.serveClientConn(remote)
		return newClientPipeline(
		local,
		blk,
		64*1024*1024, // 64MB max for unack'd bytes in flight
		func() {
			local.Close()  // kill in-memory pipe when finished
		},
	)
		
	}
	// return write session over the netwrok
	
	// find host
	raddr, err := dc.VolHost(pickVol(blk.Volumes))
	if err != nil {
		return nil,err
	}
	// get conn
	conn,err := dc.pool.getConn(raddr)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil,err
	}
	return newClientPipeline(
		conn,
		blk,
		64*1024*1024, // 64MB max for unack'd bytes in flight
		func() {
			dc.pool.returnConn(raddr,conn) // return to pool on done
		},
	)
}

func (dc *DataClient) withConn(volId uint32, f func(d Endpoint) error) error {
	// normal case, remote dataserver
	raddr, err := dc.VolHost(volId)
	if err != nil {
		return err
	}
	return dc.pool.withConn(raddr, f)
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

// pool impl
type connPool struct {
	pool           map[*net.TCPAddr]chan Endpoint
	l              *sync.RWMutex
	maxPerKey      int
	destroyOnError bool
}

func newConnPool(maxPerKey int, destroyOnError bool) *connPool {
	return &connPool{make(map[*net.TCPAddr]chan Endpoint), &sync.RWMutex{}, maxPerKey, destroyOnError}
}

// optimized to only acquire lock and perHost channel once, top and bottom of this are identical to getConn, returnConn
func (p *connPool) withConn(host *net.TCPAddr, with func(c Endpoint) error) (err error) {
	//fmt.Printf("Doing something with conn to host %s\n", host.String())
	// get chan
	p.l.RLock()
	ch, exists := p.pool[host]
	p.l.RUnlock()
	if !exists {
		p.l.Lock()
		ch, exists = p.pool[host]
		if !exists {
			ch = make(chan Endpoint, p.maxPerKey)
			p.pool[host] = ch
		}
		p.l.Unlock()
	}

	// get object
	var conn Endpoint
	select {
	case conn = <-ch:
		// nothing to do
	default:
		// create new one
		//fmt.Printf("Creating new conn to %s\n", host.String())
		conn, err = p.dial(host)
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
			return err
		}
	}
	// do our stuff
	err = with(conn)
	if err != nil {
		// don't re-use conn on error, might be crap left in the pipe
		conn.Close()
		return err
	} else {
		// return	to pool
		select {
		case ch <- conn:
			// successfully added back to freelist
			//fmt.Printf("Added conn back to pool for host %s, local %s\n", host.String(), conn)
		default:
			// freelist full, dispose of that trash
			//fmt.Printf("Closing conn for host %s, local %s\n", host.String(), conn)
			_ = conn.Close()
		}
	}
	return err
}


// direct access to conn pool, be careful to return!
func (p *connPool) getConn(host *net.TCPAddr) (Endpoint, error) {
	//fmt.Printf("Doing something with conn to host %s\n", host.String())
	// get chan
	p.l.RLock()
	ch, exists := p.pool[host]
	p.l.RUnlock()
	if !exists {
		p.l.Lock()
		ch, exists = p.pool[host]
		if !exists {
			ch = make(chan Endpoint, p.maxPerKey)
			p.pool[host] = ch
		}
		p.l.Unlock()
	}

	// get object
	var conn Endpoint
	var err error = nil
	select {
	case conn = <-ch:
		// nothing to do
	default:
		// create new one
		//fmt.Printf("Creating new conn to %s\n", host.String())
		conn, err = p.dial(host)
		if err != nil {
			if conn != nil {
				_ = conn.Close()
			}
		}
	}
	return conn, err
}

// direct access to conn pool, be careful to return!
func (p *connPool) returnConn(host *net.TCPAddr, conn Endpoint) {
	// get chan
	p.l.RLock()
	ch, exists := p.pool[host]
	p.l.RUnlock()
	if !exists {
		p.l.Lock()
		ch, exists = p.pool[host]
		if !exists {
			ch = make(chan Endpoint, p.maxPerKey)
			p.pool[host] = ch
		}
		p.l.Unlock()
	}
	// return	to pool
	select {
		case ch <- conn:
			// successfully added back to freelist
			//fmt.Printf("Added conn back to pool for host %s, local %s\n", host.String(), conn)
		default:
			// freelist full, dispose of that trash
			//fmt.Printf("Closing conn for host %s, local %s\n", host.String(), conn)
			_ = conn.Close()
	}
}

func (p *connPool) dial(host *net.TCPAddr) (Endpoint, error) {
	conn, err := net.DialTCP("tcp", nil, host)
	if err != nil {
		return nil, err
	}
	conn.SetNoDelay(true)
	//fmt.Printf("Connected to host %s with local conn %s\n",conn.RemoteAddr().String(),conn.LocalAddr().String())
	return BlockingSockEndPoint(conn)
}
