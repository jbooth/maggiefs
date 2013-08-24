package dataserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"math/rand"
	"net"
	"sync"
)

type DataClient struct {
	names   maggiefs.NameService
	volMap  map[uint32]*net.TCPAddr
	volLock *sync.RWMutex // guards volMap
	pool    *connPool
}

func NewDataClient(names maggiefs.NameService, connsPerDn int) (*DataClient, error) {
	return &DataClient{names, make(map[uint32]*net.TCPAddr), &sync.RWMutex{}, newConnPool(connsPerDn, true)}, nil
}

var (
	// compile-time check for type safety
	checkClnt maggiefs.DataService = &DataClient{}
)

func pickVol(vols []uint32) uint32 {
	return vols[rand.Int()%len(vols)]
}

// read some bytes
func (dc *DataClient) Read(blk maggiefs.Block, p []byte, pos uint64, length uint32) (err error) {
	return dc.withConn(pickVol(blk.Volumes), func(d Endpoint) error {
		// send req
		header := RequestHeader{OP_READ, blk, pos, length}
//		fmt.Printf("data client writing read header to %s\n", d)
//		fmt.Printf("Reading length %d to slice of length %d\n", length, len(p))
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
		// read resp bytes
		//fmt.Printf("Entering loop to read %d bytes\n",length)
		numRead := 0
		for uint32(numRead) < length {
//			fmt.Printf("Reading %d bytes from socket %s into slice [%d:%d]\n", length - uint32(numRead), d, numRead, int(length))
//			fmt.Printf("Slice length %d capacity %d\n",len(p),cap(p))
			n, err := d.Read(p[numRead:int(length)])
//			fmt.Printf("Read returned %d bytes, first 5: %x\n",n,p[numRead:numRead+5])
			if err != nil {
				return err
			}
			numRead += n
		}
		return nil
	})
}

// write some bytes, extending block if necessary
// updates generation ID on datanodes before returning
// if generation id doesn't match prev generation id, we have an error

func (dc *DataClient) Write(blk maggiefs.Block, p []byte, pos uint64) (err error) {
	return dc.withConn(pickVol(blk.Volumes), func(d Endpoint) error {
		// send req header
		header := &RequestHeader{OP_WRITE, blk, pos, uint32(len(p))}
		if d == nil {
			fmt.Println("nil dnconn!")
		}
		header.WriteTo(d)
		// send req bytes
		numWritten := 0
		for numWritten < len(p) {
//			fmt.Printf("Writing bytes from pos %d, first byte %x\n", numWritten, p[numWritten])
			n, err := d.Write(p[numWritten:])
			if err != nil {
				return err
			}
			numWritten += n
		}
		// read resp header
		resp := &ResponseHeader{}
		_, err := resp.ReadFrom(d)
		if resp.Stat != STAT_OK {
			return fmt.Errorf("Error code %d from DN", resp.Stat)
		}
		return err

	})
	return nil
}

func (dc *DataClient) withConn(volId uint32, f func(d Endpoint) error) error {
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
			return fmt.Errorf("No dn for vol id %d", volId)
		}
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

func (p *connPool) withConn(host *net.TCPAddr, with func(c Endpoint) error) (err error) {
	//fmt.Printf("Doing something with conn to host %s\n", host.String())
	// get chan
	p.l.RLock()
	ch, exists := p.pool[host]
	p.l.RUnlock()
	if !exists {
		p.l.Lock()
  	ch,exists = p.pool[host]	
  	if ! exists {
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

func (p *connPool) dial(host *net.TCPAddr) (Endpoint, error) {
	conn, err := net.DialTCP("tcp", nil, host)
	if err != nil {
		return nil, err
	}
	conn.SetNoDelay(true)
	//fmt.Printf("Connected to host %s with local conn %s\n",conn.RemoteAddr().String(),conn.LocalAddr().String())
	return SockEndpoint(conn),nil
}
