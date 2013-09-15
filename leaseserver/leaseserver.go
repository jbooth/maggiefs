package leaseserver

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/jbooth/maggiefs/mrpc"
	"net"
	"sync"
	"sync/atomic"
	"time"
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
	id          uint64 // unique id for this client
	c           *net.TCPConn
	d           *gob.Decoder
	e           *gob.Encoder
	req         chan queuedServerRequest // pointer to main server request channel
	resp        chan response            // client-specific, feeds responses to this client
	ackLock     *sync.Mutex
	pendingAcks map[uint64]chan bool
}

func (c *clientConn) String() string {
	return fmt.Sprintf("%+v", c)
}

func (c *clientConn) readRequests() {
	for {
		req := request{}
		err := c.d.Decode(&req)
		if err != nil {
			fmt.Printf("error reading from conn id %d, remote adr %s : %s shutting down\n", c.id, c.c.RemoteAddr().String(), err)
			// tell server to expire all our leases
			// server will then call c.closeAndDie when done
			req.Op = OP_CLOSE
			c.req <- queuedServerRequest{req, c}
			return
		}
		c.req <- queuedServerRequest{req, c}
	}

}

func (c *clientConn) sendResponses() {
	for {
		resp, ok := <-c.resp
		if !ok {
			fmt.Printf("connection %d closed, sendResponseThread dying\n")
			return
		}
		err := c.e.Encode(resp)
		if err != nil {
			// should prob signal close here
			fmt.Printf("error writing to conn %s\n", err)
		}
	}
}

// called when the server receives an ack
func (c *clientConn) ack(ackId uint64) error {
	c.ackLock.Lock()
	defer c.ackLock.Unlock()
	ackChan, exists := c.pendingAcks[ackId]
	if !exists {
		return fmt.Errorf("No pending ack with id %d for client %d", ackId, c.id)
	}
	ackChan <- true
	close(ackChan)
	delete(c.pendingAcks, ackId)
	return nil
}

// sends a notification, returning a pendingAck which will wait for acknowledgement
func (c *clientConn) notify(nodeid uint64, leaseid uint64, ackId uint64) pendingAck {
	// send notification to client
	r := response{
		Reqno:   ackId,
		Leaseid: leaseid,
		Inodeid: nodeid,
		Status:  STATUS_NOTIFY,
	}
	fmt.Printf("Sending notify %+v\n", r)
	c.resp <- r
	// create pending ack
	ack := pendingAck{c, ackId, make(chan bool, 2)}
	c.ackLock.Lock()
	c.pendingAcks[ackId] = ack.ack
	c.ackLock.Unlock()
	return ack
}

// closes all resources and releases all
func (c *clientConn) closeAndDie() {
	fmt.Printf("killing conn id %d", c.id)
	if c.resp != nil {
		close(c.resp)
		c.resp = nil
	}
	c.ackLock.Lock()
	defer c.ackLock.Unlock()
	// any writes waiting for us to acknowledge, we're never gonna so do it anyways
	for _, ackChan := range c.pendingAcks {
		ackChan <- true
		close(ackChan)
	}
	c.c.Close()
}

type pendingAck struct {
	c     *clientConn
	ackId uint64
	ack   chan bool
}

func (p pendingAck) waitAcknowledged() {
	// timeout hardcoded for now
	fmt.Printf("Waiting acknowledged conn id %d ackid %d\n", p.c.id, p.ackId)
	timeout := time.After(time.Second * 5)
	select {
	case <-p.ack:
		return
	case <-timeout:
		// client lease is EXPIRED, KILL IT
		fmt.Printf("Conn %d timed out waiting for ackid %d\n", p.c.id, p.ackId)
		p.c.closeAndDie()
	}
	return
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
		id:          incrementAndGet(&ls.clientIdCounter, 1),
		c:           raw,
		d:           gob.NewDecoder(raw),
		e:           gob.NewEncoder(raw),
		req:         ls.req,
		resp:        make(chan response, 20),
		ackLock:     new(sync.Mutex),
		pendingAcks: make(map[uint64]chan bool),
	}
	// send client id
	fmt.Printf("sending id %d\n", ret.id)
	idBuff := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(idBuff, ret.id)
	ret.c.Write(idBuff)
	fmt.Println("sent")
	return ret, nil
}

type LeaseServer struct {
	req             chan queuedServerRequest
	leasesByInode   map[uint64][]lease
	leasesById      map[uint64]lease
	leaseIdCounter  uint64
	clientIdCounter uint64
	ackIdCounter    uint64
	server          *mrpc.CloseableServer
}

// new lease server listening on bindAddr
// bindAddr should be like 0.0.0.0:9999
func NewLeaseServer(bindAddr string) (*LeaseServer, error) {

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
	ls.server = mrpc.NewCloseServer(listener, func(conn *net.TCPConn) {
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
	return ls, err
}

type queuedServerRequest struct {
	req  request
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
			err = ls.commitWriteLease(qr.req, qr.conn)
			if err != nil {
				fmt.Printf("Error committing writelease: %s\n", err.Error())
			}
			// commit will dispatch a goroutine and eventually queue OP_WRITELEASE_DONE
			continue
		case OP_WRITELEASE_RELEASE_DONE:
			// this comes from a previous invocation of WRITELEASE_RELEASE
			// response finally goes to the original calling client
			resp, err = ls.releaseLease(qr.req, qr.conn)
		case OP_ACKNOWLEDGE:
			fmt.Printf("got ack for client id %d, ackid %d\n", qr.conn.id, qr.req.Leaseid)
			qr.conn.ack(qr.req.Leaseid)
			// no response
			continue
		case OP_CHECKLEASES:
			resp, err = ls.checkLeases(qr.req, qr.conn)
		case OP_CLOSE:
			err = ls.releaseAll(qr.conn)
			qr.conn.closeAndDie()
			if err != nil {
				fmt.Printf("Error closing conn: %s", err)
			}
			continue // don't send a response
		default:
			err = fmt.Errorf("Bad request num %d", qr.req.Op)
		}
		// send responses
		if err != nil {
			fmt.Printf("error processing request %+v, error: %s", qr.req, err)
			qr.conn.resp <- response{0, 0, 0, STATUS_ERR}
		} else {
			qr.conn.resp <- resp
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
					fmt.Printf("Write lease already taken for inode %d, leases : %+v\n", r.Inodeid, ls.leasesByInode[r.Inodeid])
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
	// remove from lease->inode map
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

// release all belonging to a given client
func (ls *LeaseServer) releaseAll(c *clientConn) error {
	fmt.Printf("Releasing all belonging to client %d\n", c.id)
	// iterate all leases
	for _, inodeLeases := range ls.leasesByInode {
		for idx, lease := range inodeLeases {
			fmt.Printf("Checking if lease on inode %d belongs to our client :  %d == %d\n", lease.inodeid, lease.client.id, c.id)
			if lease.client.id == c.id {
				fmt.Printf("Deleting\n")
				// belongs to this client so kill it
				delete(ls.leasesById, lease.leaseid)
				ls.leasesByInode[lease.inodeid] = append(inodeLeases[:idx], inodeLeases[idx+1:]...)
				if len(ls.leasesByInode[lease.inodeid]) == 0 {
					delete(ls.leasesByInode, lease.inodeid)
				}
			}
		}
	}
	return nil
}

func (ls *LeaseServer) commitWriteLease(r request, c *clientConn) error {
	// find all readleases attached to this inode id
	readLeases := ls.leasesByInode[r.Inodeid]
	readLeasesWithoutCommitter := make([]lease, 0, len(readLeases))
	// omit the connection that sent the commit request -- we don't want to blow our own cache while writing
	for _, rl := range readLeases {
		if rl.client.id != c.id && !rl.writeLease {
			readLeasesWithoutCommitter = append(readLeasesWithoutCommitter, rl)
		}
	}

	pendingAcks := make([]pendingAck, len(readLeasesWithoutCommitter))
	// notify them all
	idx := 0
	for _, rl := range readLeasesWithoutCommitter {
		ls.ackIdCounter += 1
		pendingAcks[idx] = rl.client.notify(r.Inodeid, rl.leaseid, ls.ackIdCounter)
	}
	go func() {
		// wait all readers acknowledged
		for _, ack := range pendingAcks {
			ack.waitAcknowledged()
		}
		// queue response to be sent to writer
		r.Op = OP_WRITELEASE_RELEASE_DONE
		ls.req <- queuedServerRequest{r, c}
	}()
	return nil
}

func (ls *LeaseServer) checkLeases(r request, c *clientConn) (response, error) {
	inodeLeases := ls.leasesByInode[r.Inodeid]
	fmt.Printf("Checking leases for inode %d : %+v\n", r.Inodeid, inodeLeases)
	if inodeLeases != nil && len(inodeLeases) > 0 {
		return response{r.Reqno, r.Leaseid, r.Inodeid, STATUS_WAIT}, nil
	}
	return response{r.Reqno, r.Leaseid, r.Inodeid, STATUS_OK}, nil
}

// atomically adds incr to val, returns new val
func incrementAndGet(val *uint64, incr uint64) uint64 {
	currVal := atomic.LoadUint64(val)
	for !atomic.CompareAndSwapUint64(val, currVal, currVal+incr) {
		currVal = atomic.LoadUint64(val)
	}
	return currVal + incr
}
