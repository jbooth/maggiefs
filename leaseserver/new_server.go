package leaseserver

import (
	"encoding/gob"
	"fmt"
	"net"
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
	c      *net.TCPConn
	d      *gob.Decoder
	e      *gob.Encoder
	req    chan queuedServerRequest // pointer to main server request channel
	resp   chan response            // client-specific, feeds responses to this client
	closed chan bool
}

func (c *clientConn) readRequests() {
	for {
		req := request{}
		err := c.d.Decode(&req)
		if err != nil {
			// we should probably close here
			fmt.Printf("error reading from conn %s\n", err)
		}
		c.req <- queuedServerRequest{req, c.resp, c}
	}

}

func (c *clientConn) sendResponses() {
	for {
		resp := <-c.resp
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
	return &clientConn{
		c:    raw,
		d:    gob.NewDecoder(raw),
		e:    gob.NewEncoder(raw),
		req:  ls.req,
		resp: make(chan response, 10)}, nil
}

type LeaseServer struct {
	sock           *net.TCPListener
	newConns       chan *clientConn
	req            chan queuedServerRequest
	leasesByInode  map[uint64][]lease
	leasesById     map[uint64]lease
	leaseIdCounter uint64
}

func NewLeaseServer(port int) (*LeaseServer,error) {
  
  laddr,err := net.ResolveTCPAddr("tcp",fmt.Sprintf("0.0.0.0:%d",port))
  if err != nil { return nil,err }
  
  listener,err := net.ListenTCP("tcp",laddr)
  if err != nil { return nil,err }
  
  
  return &LeaseServer{listener, 
    make(chan *clientConn), make(chan queuedServerRequest), make(map[uint64] []lease),
    make(map[uint64] lease),0},nil
}

type queuedServerRequest struct {
	req  request
	resp chan response
	conn *clientConn
}

func (ls *LeaseServer) Serve() {
	// dispatch process() to handle requests from connections
	go ls.process()
	// for new connection
	for {
		tcpConn, err := ls.sock.AcceptTCP()
		if err != nil {
			fmt.Printf("error accepting connection: %s\n", err)
			return
		}
		// instantiate conn object
		client, err := newClientConn(ls, tcpConn)
		if err != nil {
			fmt.Printf("error wrapping clientConn %s\n", err)
		}
		// launch its goroutines
		go client.readRequests()
		go client.sendResponses()
	}
}

func (ls *LeaseServer) process() {
	// pull requests
	for {
		qr := <-ls.req
		var resp response
		var err error
		// execute
		switch qr.req.op {
		case OP_READLEASE:
			resp, err = ls.createLease(qr.req, qr.conn, false)
		case OP_WRITELEASE:
			resp, err = ls.createLease(qr.req, qr.conn, true)
		case OP_READLEASE_RELEASE:
			resp, err = ls.releaseLease(qr.req, qr.conn)
		case OP_WRITELEASE_RELEASE:
			resp, err = ls.releaseLease(qr.req, qr.conn)
		case OP_WRITELEASE_COMMIT:
			resp, err = ls.commitWriteLease(qr.req, qr.conn)
	  case OP_CHECKLEASES:
	    resp,err = ls.checkLeases(qr.req,qr.conn)
		default:
			err = fmt.Errorf("Bad request num %d", qr.req.op)
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

func (ls *LeaseServer) createReadLease(r request, c *clientConn) (response, error) {
	// get new lease
	ls.leaseIdCounter += 1
	leaseid := ls.leaseIdCounter
	l := lease{leaseid, r.inodeid, false, c}
	// record lease in ls.inodeLeases under inode id
	leasesForInode, exists := ls.leasesByInode[r.inodeid]
	if !exists {
		leasesForInode = make([]lease, 10, 10)
	}
	ls.leasesByInode[r.inodeid] = append(leasesForInode, l)
	// record in ls.leases under lease Id
	ls.leasesById[l.leaseid] = l
	return response{r.reqno, leaseid, r.inodeid, STATUS_OK}, nil
}

func (ls *LeaseServer) createLease(r request, c *clientConn, isWriteLease bool) (response, error) {
	// if write request, we can only have one taken at a time
	if isWriteLease {
		writeLeaseAlreadyTaken := false
		for _, l := range ls.leasesByInode[r.inodeid] {
			if l.writeLease {
				writeLeaseAlreadyTaken = true
				break
			}
		}
		if writeLeaseAlreadyTaken {
			// return wait response
			return response{r.reqno, 0, 0, STATUS_WAIT}, nil
		}
	}

	// else generate new lease
	ls.leaseIdCounter += 1
	leaseid := ls.leaseIdCounter
	l := lease{leaseid, r.inodeid, isWriteLease, c}
	// record lease in ls.inodeLeases under inode id
	leasesForInode, exists := ls.leasesByInode[r.inodeid]
	if !exists {
		leasesForInode = make([]lease, 10, 10)
	}
	leasesForInode = append(leasesForInode, l)
	// record in ls.leases under lease Id
	ls.leasesById[l.leaseid] = l
	return response{r.reqno, leaseid, r.inodeid, STATUS_OK}, nil
}

func (ls *LeaseServer) releaseLease(r request, c *clientConn) (response, error) {
	// find inode id
	inodeid := ls.leasesById[r.leaseid].inodeid
	// find readleases
	inodeLeases := ls.leasesByInode[r.inodeid]
	// remove from lease map 
	delete(ls.leasesById, r.leaseid)
	// find in inode leaselist
	for idx,val := range inodeLeases {
	 if val.leaseid == r.leaseid {
	   // remove from list
     copy(inodeLeases[idx:],inodeLeases[idx+1:]) // copy idx+1-> end to positions starting at idx
	   break
	 }
	}
	// clean up list if empty
	if len(inodeLeases) == 0 {
	 delete(ls.leasesByInode,inodeid)
	}
	// done
	return response{0, 0, 0, STATUS_OK}, nil
}

func (ls *LeaseServer) commitWriteLease(r request, c *clientConn) (response, error) {
	// find all readleases attached to this inode id
	readLeases := ls.leasesByInode[r.inodeid]
  
	// notify them all
	for _, rl := range readLeases {
		if rl.leaseid != r.leaseid {
			rl.client.resp <- response{0, rl.leaseid, r.inodeid, STATUS_NOTIFY}
		}
	}
	return response{r.reqno, r.leaseid, r.inodeid, STATUS_OK}, nil
}

func (ls *LeaseServer) checkLeases(r request, c *clientConn) (response,error) {
  inodeLeases := ls.leasesByInode[r.inodeid]
  if inodeLeases != nil && len(inodeLeases) > 0{
    return response{r.reqno,r.leaseid,r.inodeid,STATUS_WAIT},nil
  }
  return response{r.reqno,r.leaseid,r.inodeid,STATUS_OK},nil 
}

func (ls *LeaseServer) Close() {
  
}
