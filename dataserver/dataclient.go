package dataserver

import (
	"net"
	"github.com/jbooth/maggiefs/maggiefs"
	"sync"
)

type Dataclient struct {
	names maggiefs.NameService
	volMap map[int32] *net.TCPAddr
	volLock *sync.RWMutex
}

	// read some bytes
func (ds *Dataclient) Read(blk maggiefs.Block, p []byte, pos uint64, length uint64) (err error) {
	return nil
}

  // write some bytes, extending block if necessary
  // updates generation ID on datanodes before returning
  // if generation id doesn't match prev generation id, we have an error
  
func (dc *Dataclient)  Write(blk maggiefs.Block, p []byte, pos uint64) (err error) {
	return nil
}
