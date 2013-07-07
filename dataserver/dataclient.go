package dataserver

import (
	"net"
	"github.com/jbooth/maggiefs/maggiefs"
	"sync"
	"fmt"
	"math/rand"
)

type DataClient struct {
	names maggiefs.NameService
	volMap map[uint32] *net.TCPAddr
	volLock *sync.RWMutex // guards volMap
	pool *connPool
}

func NewDataClient(names maggiefs.NameService, connsPerDn int) (*DataClient,error) {
	return &DataClient{names,make(map[uint32]*net.TCPAddr),&sync.RWMutex{},newConnPool(connsPerDn,true)},nil
}

var (
	// compile-time check for type safety
	checkClnt maggiefs.DataService = &DataClient{}
)

func pickVol(vols []uint32) uint32 {
	return vols[rand.Int() % len(vols)]
}
	// read some bytes
func (dc *DataClient) Read(blk maggiefs.Block, p []byte, pos uint64, length uint32) (err error) {
	return dc.withConn(pickVol(blk.Volumes),func(d *dnConn) error {
		// send req
		header := RequestHeader{OP_READ,blk,pos,length}
		fmt.Printf("data client writing read header to %s, local addr %s\n",d.c.RemoteAddr().String(),d.c.LocalAddr().String())
		fmt.Printf("Reading length %d to slice of length %d\n",length,len(p))
		_,err = header.WriteTo(d.c)
		if err != nil {
			return fmt.Errorf("Error writing header to dn : %s",err.Error())
		}
		fmt.Println("dataclientRead:  wrote header")
		// read resp header
		resp := &ResponseHeader{}
		fmt.Println("reading response header")
		_,err = resp.ReadFrom(d.c)
		if err != nil {
			return fmt.Errorf("Error reading header from dn : %s",err.Error())
		}
		if resp.Stat == STAT_ERR {
			return fmt.Errorf("Error code %d from DN",resp.Stat)
		} 
		// read resp bytes
		numRead := 0
		for ; uint32(numRead) < length ; {
			
			n,err := d.c.Read(p[numRead:int(length)])
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
  
func (dc *DataClient)  Write(blk maggiefs.Block, p []byte, pos uint64) (err error) {
	return dc.withConn(pickVol(blk.Volumes), func(d *dnConn) error {
		// send req header
		header := &RequestHeader{OP_WRITE,blk,pos,uint32(len(p))}
		if d == nil {
			fmt.Println("nil dnconn!")
		}
		header.WriteTo(d.c)
		fmt.Printf("wrote header, writing bytes first 5: %x\n",p[:5])
		// send req bytes
		numWritten := 0
		for ; numWritten <  len(p) ; {
			n,err := d.c.Write(p[numWritten:])
			if err != nil {
				return err
			}
			fmt.Printf("wrote %d bytes\n",n)
			numWritten += n
		}
		// read resp header
		resp := &ResponseHeader{}
		_,err := resp.ReadFrom(d.c)
		if resp.Stat != STAT_OK {
			return fmt.Errorf("Error code %d from DN",resp.Stat)
		}
		return err
		
	})
	return nil
}

type dnConn struct {
	c *net.TCPConn
}

func (dc *DataClient) withConn(volId uint32, f func(d *dnConn) error) error {
	dc.volLock.RLock()
	raddr,exists := dc.volMap[volId]
	dc.volLock.RUnlock()
	if !exists {
		// try refreshing map and try again
		dc.refreshDNs()
		dc.volLock.RLock()
		raddr,exists = dc.volMap[volId]
		dc.volLock.RUnlock()
		if ! exists {
			return fmt.Errorf("No dn for vol id %d",volId)
		}
	}
	return dc.pool.withConn(raddr,f)
}

func (dc *DataClient) refreshDNs() error {
	dc.volLock.Lock()
	defer dc.volLock.Unlock()
	fsStat,err := dc.names.StatFs()
	if err != nil { return err }
	// clear existing map
	dc.volMap =  make(map[uint32]*net.TCPAddr)
	// fill it up
	for _,dnStat := range fsStat.DnStat {
		dnAddr,err := net.ResolveTCPAddr("tcp",dnStat.Addr)
		if err != nil { return err }
		for _,volStat := range dnStat.Volumes {
			dc.volMap[volStat.VolId] = dnAddr
		}
	}
	return nil
}

// pool impl
type connPool struct {
	pool map[*net.TCPAddr] chan *dnConn
	l *sync.RWMutex
	maxPerKey int
	destroyOnError bool
}

func newConnPool(maxPerKey int, destroyOnError bool) *connPool {
	return &connPool{make(map[*net.TCPAddr] chan *dnConn),&sync.RWMutex{},maxPerKey,destroyOnError}
}

func (p *connPool) withConn(host *net.TCPAddr, with func(c *dnConn) error) (err error) {
	fmt.Printf("Doing something with conn to host %s\n",host.String())
	// get chan
	p.l.RLock()
	ch,exists := p.pool[host]
	p.l.RUnlock()
	if !exists {
		ch = make(chan *dnConn,p.maxPerKey)
		p.l.Lock()
		p.pool[host] = ch
		p.l.Unlock()
	}
	
	// get object
	var conn *dnConn
	select {
		case conn = <- ch:
		  // nothing to do
		default:
		  // create new one
		  conn,err = p.dial(host)
		  if err != nil {
		  	if conn != nil {
			  	_ = conn.c.Close()
		  	}
		  	return err
		  }
	}
	
	// do our stuff
	err = with(conn)
	if err != nil {
		// don't re-use conn on error, might be crap left in the pipe
		conn.c.Close()
		return err
	}
	// return	to pool
	select {
		//case ch <- conn:
		  // successfully added back to freelist
		default:
		  // freelist full, dispose of that trash
		  _ = conn.c.Close()
	}
	return err
}

func (p *connPool) dial(host *net.TCPAddr) (*dnConn,error) {
	conn,err := net.DialTCP("tcp",nil,host)
	if err != nil { return nil,err }
	conn.SetNoDelay(true)
	return &dnConn { conn },nil
}