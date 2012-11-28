package leaseserver

import (
	"encoding/gob"
  "github.com/jbooth/maggiefs/maggiefs"
	"fmt"
	"net"
)

type rawclient struct {
	c          *net.TCPConn
	reqcounter uint32
	notifier   chan uint64
	requests   chan queuedRequest
	responses  chan response
	closeMux   chan bool
	//closeResponseReader chan bool
}

type queuedRequest struct {
	r        request
	whenDone chan response
}

func newRawClient(addr string) (*rawclient, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return nil, err
	}
	c, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	c.SetNoDelay(true)
	c.SetKeepAlive(true)
	return &rawclient{c, 0, make(chan uint64, 100), make(chan queuedRequest), make(chan response), make(chan bool)}, nil
}

func (c *rawclient) doRequest(r request) (response, error) {
	respChan := make(chan response)
	q := queuedRequest{r, respChan}
	c.requests <- q
	resp := <-respChan
	return resp, nil
}

func (c *rawclient) mux() {
	responseChans := make(map[uint32]chan response)
	reqEncoder := gob.NewEncoder(c.c)
	for {
		select {
		case req := <-c.requests:
			// register response channel
			c.reqcounter++
			req.r.reqno = c.reqcounter
			responseChans[req.r.reqno] = make(chan response)
			// write the req to socket
			err := reqEncoder.Encode(req)
			if err != nil {
				fmt.Println("error encoding req %+v : %s", req, err.Error())
				continue
			}
		case resp := <-c.responses:
			if resp.status == STATUS_NOTIFY {
			  // this is a notification so forward to the notification chan
			  c.notifier <- resp.leaseid  // lease id is overridden here with inode id
			} else {
				// response to a request, forward to it's response chan
				k := resp.reqno
				respChan := responseChans[k]
				delete(responseChans, k)
				respChan <- resp
				close(respChan)
			}  
		case _ = <-c.closeMux:
			return
		}
	}
}

// todo figure out timeouts so that this thing actually dies
func (c *rawclient) readResponses() {
	respDecoder := gob.NewDecoder(c.c)
	for {
    resp := response{}
    respDecoder.Decode(&resp)
    c.responses <- resp  
	}
}

  // acquires the write lease for the given inode
  // only one client may have the writelease at a time, however it is pre-emptable in case 
  // a higher priority process (re-replication etc) needs this lease.
  // on pre-emption, the supplied commit() function will be called
  // pre-emption will not happen while WriteLease.ShortTermLock() is held, however that lock should 
  // not be held for the duration of anything blocking
  func (c *rawclient) WriteLease(nodeid uint64) (l maggiefs.WriteLease, err error) {
    return nil,nil
  }
  
  // takes out a lease for an inode, this is to keep the posix convention that unlinked files
  // aren't cleaned up until they've been closed by all programs
  // also registers a callback for when the node is remotely changed, this will be triggered
  // upon the file changing *unless* we've cancelled this lease.  Recommend 
  func (c *rawclient) ReadLease(nodeid uint64) (l maggiefs.ReadLease, err error) {
    return nil,nil
  }
  
  // returns a chan which will contain an event every time any inode in the system is changed
  // used for cache coherency
  // the fuse client runs a goroutine reading all changes from this chan
  func (c *rawclient) GetNotifier() chan uint64 {
    return nil
  }
  
  // blocks until all leases are released for the given node
  func (c *rawclient) WaitAllReleased(nodeid uint64) error {
    return nil
  }


