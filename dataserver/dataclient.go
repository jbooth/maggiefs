package dataserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"math/rand"
	"net"
	"os"
	"sync"
  "log"
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
	return &DataClient{names, make(map[uint32]*net.TCPAddr), &sync.RWMutex{}, &connPool{make(map[*net.TCPAddr]*RawClient), new(sync.RWMutex)}, nil, nil}, nil
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

  // we have 2 methods to read, in order to optimize by avoiding a context switch for singleblock reads
    //ReadNoCommit(blk Block, buf SplicerTo, pos uint32, length uint32, onDone chan bool) error
      //ReadCommit(blk Block, buf SplicerTo, pos uint32, length uint32) error 



func (dc *DataClient) ReadCommit (blk maggiefs.Block, buf maggiefs.SplicerTo, pos uint64, length uint32) error {
  return dc.doRead(blk,buf,pos,length,func() {
    err := buf.Commit()
    if err != nil {
      log.Printf("Err committing buff in dataclient.ReadCommit: %s",err)
    }
  })
}

func (dc *DataClient) ReadNoCommit(blk maggiefs.Block, buf maggiefs.SplicerTo, pos uint64, length uint32, onDone chan bool) error {
  return dc.doRead(blk,buf,pos,length,func() {
    onDone <- true
  })

}

// does the read and calls onDone in receiver thread after splicing to SplicerTo
func (dc *DataClient) doRead(blk maggiefs.Block, buf maggiefs.SplicerTo, pos uint64, length uint32, onDone func()) error {
  if dc.isLocal(blk.Volumes) {
    // local direct read
    return dc.localDS.DirectRead(blk, buf, pos, length, onDone)
  }
  host, err := dc.VolHost(pickVol(blk.Volumes))
  if err != nil {
    return nil
  }

  conn, err := dc.pool.getConn(host)
  if err != nil {
    return err
  }
  header := RequestHeader{OP_READ, 0, blk, pos, length}
  conn.DoRequest(header, nil, func(d *os.File) {
    // read resp bytes
    //fmt.Printf("Entering loop to read %d bytes\n",length)
    numRead := 0
    for uint32(numRead) < length {
      fmt.Printf("Reading %d bytes from socket %s into slice [%d:%d]\n", length-uint32(numRead), d, numRead, int(length))
      //      fmt.Printf("Slice length %d capacity %d\n",len(p),cap(p))
      n, err := buf.SpliceBytes(d.Fd(), int(length)-numRead)
      //      fmt.Printf("Read returned %d bytes, first 5: %x\n",n,p[numRead:numRead+5])
      if err != nil {
        fmt.Printf("Error while splicing: %s\n", err)
        return
      }
      numRead += n
    }
    onDone()
  })
  return nil
}

func (dc *DataClient) Write(blk maggiefs.Block, p []byte, pos uint64, onDone func()) (err error) {
	// always go in order of volumes on block
	host, err := dc.VolHost(blk.Volumes[0])
	if err != nil {
		return err
	}
	conn, err := dc.pool.getConn(host)
	if err != nil {
		return err
	}
	reqHeader := RequestHeader{OP_WRITE, 0, blk, pos, uint32(len(p))}
	conn.DoRequest(reqHeader, p, func(f *os.File) {
		onDone()
	})
	return nil
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
