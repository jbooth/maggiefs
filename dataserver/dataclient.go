package dataserver

import (
	"net"
	"github.com/jbooth/maggiefs/maggiefs"
	"sync"
	"encoding/gob"
	"fmt"
	"math/rand"
)

type DataClient struct {
	names maggiefs.NameService
	volMap map[int32] *net.TCPAddr
	volLock *sync.RWMutex
	localAddr *net.TCPAddr
}

func pickVol(vols []int32) int32 {
	return vols[rand.Int() % len(vols)]
}
	// read some bytes
func (dc *DataClient) Read(blk maggiefs.Block, p []byte, pos uint64, length uint32) (err error) {
	header := RequestHeader{OP_READ,blk.Id,blk.Version,pos,length}
	return dc.withConn(pickVol(blk.Volumes),func(d *dnConn) error {
		d.e.Encode(header)
		resp := &ResponseHeader{}
		if resp.Stat == STAT_ERR {
			return fmt.Errorf(resp.Err)
		} 
		numRead := 0
		for ; uint32(numRead) < length ; {
			n,err := d.c.Read(p[numRead:int(length) - numRead])
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
	return nil
}

type dnConn struct {
	c *net.TCPConn
	d *gob.Decoder
	e *gob.Encoder
}

func (dc *DataClient) withConn(dnId int32, f func(d *dnConn) error) error {
	// todo get this thing from pool rather than reconnecting each time
	dc.volLock.RLock()
	raddr,exists := dc.volMap[dnId]
	dc.volLock.RUnlock()
	if !exists {
		// try refreshing map and try again
		dc.refreshDNs()
		dc.volLock.RLock()
		raddr,exists = dc.volMap[dnId]
		if ! exists {
			return fmt.Errorf("No dn with id %d",dnId)
		}
	}
	conn,err := net.DialTCP("tcp",dc.localAddr,raddr)
	if err != nil { return err }
	defer conn.Close()
	dnConn := &dnConn { conn, gob.NewDecoder(conn), gob.NewEncoder(conn) }
	return f(dnConn)
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
		dnAddr := net.ResolveTCPAddr("tcp",dnStat.Addr)
		for _,volStat := range dnStat.Volumes {
			dc.volMap[volStat.VolId] = dnAddr
		}
	}
	
}