package leaseserver

import (
  "github.com/jbooth/maggiefs/mrpc"
	"encoding/gob"
	"fmt"
	"net"
	"encoding/binary"
	"sync/atomic"
)

// maps

// host -> list of pointers to inode/lease pairs

// inode -> list of readleases/host + writelease/host

// on actions

// readlease()  -- set it up, return unique id
// readlease.release() -- remove from this inode with this id
// writelease() -- set it up
// writelease.release() -- remove from this inode
// writelease.commit() -- notify all readleases

// workflow

// main Serve() method accepts connections and launches their respective goroutines
// mux() pulls requests from conns and feeds them responses

type lease struct {
	leaseid    uint64
	inodeid    uint64
	writeLease bool
	client     *clientConn
}

type clientConn struct {
	id 		 uint64                   // unique id for this client
	c      *net.TCPConn
	d      *gob.Decoder
	e      *gob.Encoder
	req    chan queuedServerRequest // pointer to main server request channel
	resp   chan response            // client-specific, feeds responses to this client
	closed chan bool
}

func (c *clientConn) String() string {
	return fmt.Sprintf("%+v",c)
}

func (c *clientConn) readRequests() {
	for {
		req := request{}
		err := c.d.Decode(&req)
		if err != nil {
			// we should probably close here
			fmt.Printf("error reading from conn %s\n", err)
			c.c.Close()
			close(c.resp)
			return
		}
		c.req <- queuedServerRequest{req, c.resp, c}
	}

}

func (c *clientConn) sendResponses() {
	for {
		resp,ok := <-c.resp
		if !ok { 
			fmt.Printf("connection %d closed, sendResponseThread dying")
			return
		}
		err := c.e.Encode(resp)
		if err != nil {
			// should prob signal close here
			fmt.Printf("error writing to conn %s\n", err)
		}
	}
}

func newClientConn(ls *LeaseServer, raw *net.TCPConn) (*clientConn, error) {
	err := raw.SetKeepAlive(true)
	if err != nil {
		return nil, err
	}
	err = raw.SetNoDelay(true)
	if err != nil {
		return nil, err
	}
	ret := &clientConn{
	  id: incrementAndGet(&ls.clientIdCounter,1),
		c:    raw,
		d:    gob.NewDecoder(raw),
		e:    gob.NewEncoder(raw),
		req:  ls.req,
		resp: make(chan response, 10)}
	// send client id
	fmt.Printf("sending id %d\n",ret.id)
	idBuff := make([]byte,8,8)
	binary.LittleEndian.PutUint64(idBuff,ret.id)
	ret.c.Write(idBuff)
	fmt.Println("sent")
	return ret, nil
}

type LeaseServer struct {
	req            chan queuedServerRequest
	leasesByInode  map[uint64][]lease
	leasesById     map[uint64]lease
	leaseIdCounter uint64
	clientIdCounter uint64
	server *mrpc.CloseableServer
}

// new lease server listening on bindAddr
// bindAddr should be like 0.0.0.0:9999
func NewLeaseServer(bindAddr string) (*LeaseServer,error) {

	laddr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, err
	}

	listener, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		return nil, err
	}
	ls := &LeaseServer{}
  ls.req = make(chan queuedServerRequest)
  ls.leasesByInode = make(map[uint64][]lease)
  ls.leasesById = make(map[uint64]lease)
	ls.server = mrpc.NewCloseServer(listener,func(conn *net.TCPConn) {
	  // instantiate conn object
    client, err := newClientConn(ls, conn)
    fmt.Println("got new client")
    if err != nil {
      fmt.Printf("error wrapping clientConn %s\n", err)
    }
    // launch goroutines to serve
    
    go client.readRequests()
    go client.sendResponses()
	})
  return ls,err
}

type queuedServerRequest struct {
	req  request
	resp chan response
	conn *clientConn
}

func (ls *LeaseServer) Start() error {
	fmt.Println("lease server starting")
  go ls.process()
  ls.server.Start()
  return nil
}

func (ls *LeaseServer) Close() error {
  // TODO unwind pending lease requests?  or notify clients of shutdown?
  return ls.server.Close()
}

func (ls *LeaseServer) WaitClosed() error {
  return ls.server.WaitClosed()
}

func (ls *LeaseServer) process() {
	// pull requests
	for {
		qr := <-ls.req
		var resp response
		var err error
		// execute
		switch qr.req.Op {
		case OP_READLEASE:
			resp, err = ls.createLease(qr.req, qr.conn, false)
		case OP_WRITELEASE:
			resp, err = ls.createLease(qr.req, qr.conn, true)
		case OP_READLEASE_RELEASE:
			resp, err = ls.releaseLease(qr.req, qr.conn)
		case OP_WRITELEASE_RELEASE:
			_,err = ls.commitWriteLease(qr.req,qr.conn)
			if err != nil { break }
			resp, err = ls.releaseLease(qr.req, qr.conn)
		case OP_CHECKLEASES:
			resp, err = ls.checkLeases(qr.req, qr.conn)
		default:
			err = fmt.Errorf("Bad request num %d", qr.req.Op)
		}
		// send responses
		if err != nil {
			fmt.Printf("error processing request %+v, error: %s", qr.req, err)
			qr.resp <- response{0, 0, 0, STATUS_ERR}
		} else {
			qr.resp <- resp
		}
	}

}

func (ls *LeaseServer) createLease(r request, c *clientConn, isWriteLease bool) (response, error) {
	// if write request, we can only have one taken at a time
	if isWriteLease {
		writeLeaseAlreadyTaken := false
		leases := ls.leasesByInode[r.Inodeid]
		if leases != nil {
			for _, l := range ls.leasesByInode[r.Inodeid] {
				if l.writeLease {
					writeLeaseAlreadyTaken = true
					break
				}
			}
		}
		if writeLeaseAlreadyTaken {
			// return wait response
			return response{r.Reqno, 0, 0, STATUS_WAIT}, nil
		}
	}

	// else generate new lease
	ls.leaseIdCounter += 1
	leaseid := ls.leaseIdCounter
	l := lease{leaseid, r.Inodeid, isWriteLease, c}
	// record lease in ls.inodeLeases under inode id
	leasesForInode, exists := ls.leasesByInode[r.Inodeid]
	if !exists {
		leasesForInode = make([]lease, 0, 0)
	}
	leasesForInode = append(leasesForInode, l)
	ls.leasesByInode[r.Inodeid] = leasesForInode
	// record in ls.leases under lease Id
	ls.leasesById[l.leaseid] = l
	return response{r.Reqno, leaseid, r.Inodeid, STATUS_OK}, nil
}

func (ls *LeaseServer) releaseLease(r request, c *clientConn) (response, error) {
	// find inode id
	inodeid := ls.leasesById[r.Leaseid].inodeid
	// find readleases
	inodeLeases := ls.leasesByInode[r.Inodeid]
	// remove from lease map 
	delete(ls.leasesById, r.Leaseid)
	// find in inode leaselist
	for idx, val := range inodeLeases {
		if val.leaseid == r.Leaseid {
			// remove from list
			ls.leasesByInode[r.Inodeid] = append(inodeLeases[:idx], inodeLeases[idx+1:]...)
			break
		}
	}
	// clean up list if empty
	if len(inodeLeases) == 0 {
		delete(ls.leasesByInode, inodeid)
	}
	// done
	return response{r.Reqno, 0, 0, STATUS_OK}, nil
}

func (ls *LeaseServer) commitWriteLease(r request, c *clientConn) (response, error) {
	// find all readleases attached to this inode id
	readLeases := ls.leasesByInode[r.Inodeid]
	readLeasesWithoutCommitter := make([]lease,0,len(readLeases))
	// omit the connection that sent the commit request -- we don't want to blow our own cache while writing
	for _,rl := range readLeases {
		if rl.client.id != c.id && !rl.writeLease {
			readLeasesWithoutCommitter = append(readLeasesWithoutCommitter,rl)
		}
	}
	// notify them all
	for _, rl := range readLeasesWithoutCommitter {
		rl.client.resp <- response{0, rl.leaseid, r.Inodeid, STATUS_NOTIFY}
	}
	return response{r.Reqno, r.Leaseid, r.Inodeid, STATUS_OK}, nil
}

func (ls *LeaseServer) checkLeases(r request, c *clientConn) (response, error) {
	inodeLeases := ls.leasesByInode[r.Inodeid]
	if inodeLeases != nil && len(inodeLeases) > 0 {
		return response{r.Reqno, r.Leaseid, r.Inodeid, STATUS_WAIT}, nil
	}
	return response{r.Reqno, r.Leaseid, r.Inodeid, STATUS_OK}, nil
}

// atomically adds incr to val, returns new val
func incrementAndGet(val *uint64, incr uint64) uint64 {
  currVal := atomic.LoadUint64(val)
  for ; !atomic.CompareAndSwapUint64(val,currVal,currVal+incr) ; {
    currVal = atomic.LoadUint64(val)
  }
  return currVal + incr
}