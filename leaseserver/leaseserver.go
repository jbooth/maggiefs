package leaseserver

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// maps

// host -> list of pointers to inode/lease pairs

// inode -> list of readleases/host

// on actions

// readlease()  -- set it up, return unique id
// readlease.release() -- remove from this inode with this id
// notify() -- notify all readleases

// workflow

// main Serve() method accepts connections and launches their respective goroutines
// mux() pulls requests from conns and feeds them responses

type lease struct {
	leaseid uint64
	inodeid uint64
	client  *clientConn
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

func (c *clientConn) readRequests() error {
	for {
		req := request{}
		err := c.d.Decode(&req)
		if err != nil {
			fmt.Printf("error reading from conn id %d, remote adr %s : %s shutting down\n", c.id, c.c.RemoteAddr().String(), err)
			// tell server to expire all our leases
			// server will then call c.closeAndDie when done
			req.Op = OP_CLOSE
			c.req <- queuedServerRequest{req, c}
			return err
		}
		c.req <- queuedServerRequest{req, c}
	}
	return nil

}

func (c *clientConn) sendResponses() error {
	for {
		resp, ok := <-c.resp
		if !ok {
			return fmt.Errorf("connection %d closed, sendResponseThread dying\n")
		}
		err := c.e.Encode(resp)
		if err != nil {
			return err
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
func (c *clientConn) notify(nodeid uint64, leaseid uint64, ackId uint64, offset int64, length int64) pendingAck {
	// send notification to client
	r := response{
		Reqno:          ackId,
		Leaseid:        leaseid,
		Inodeid:        nodeid,
		Status:         STATUS_NOTIFY,
		NotifyStartPos: offset,
		NotifyLength:   length,
	}
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
	timeout := time.After(time.Second * 120)
	select {
	case <-p.ack:
		return
	case <-timeout:
		// client lease is EXPIRED, KILL IT
		log.Printf("Conn %d timed out after 120s waiting for ackid %d\n", p.c.id, p.ackId)
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
}

// new lease server listening on bindAddr
// bindAddr should be like 0.0.0.0:9999
func NewLeaseServer() *LeaseServer {
	ls := &LeaseServer{}
	ls.req = make(chan queuedServerRequest)
	ls.leasesByInode = make(map[uint64][]lease)
	ls.leasesById = make(map[uint64]lease)
	go ls.process()
	return ls
}

func (ls *LeaseServer) ServeConn(conn *net.TCPConn) {
	// instantiate conn object
	client, err := newClientConn(ls, conn)
	fmt.Println("got new client")
	if err != nil {
		fmt.Printf("error wrapping clientConn %s\n", err)
	}
	// launch goroutines to serve
	go func() {
		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic on LeaseServ reading requests for client %s : %s, shutting down \n", client.String(), x)
				client.closeAndDie()
			}
		}()
		client.readRequests()
	}()
	go func() {
		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic on LeaseServ sending responses from client %s, shutting down \n", client.String(), x)
				client.closeAndDie()
			}
		}()
		client.sendResponses()
	}()
}

type queuedServerRequest struct {
	req  request
	conn *clientConn
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
			resp, err = ls.createLease(qr.req, qr.conn)
		case OP_NOTIFY:
			err = ls.notify(qr.req, qr.conn)
			// no response from this loop, notify spins off a goroutine to respond directly to qr.conn
			continue
		case OP_READLEASE_RELEASE:
			resp, err = ls.releaseLease(qr.req, qr.conn)
			if err != nil {
				log.Printf("Error releasing readlease: %s\n", err.Error())
			}
		case OP_ACKNOWLEDGE:
			//fmt.Printf("got ack for client id %d, ackid %d\n", qr.conn.id, qr.req.Leaseid)
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
			qr.conn.resp <- response{0, 0, 0, STATUS_ERR, 0, 0}
		} else {
			//fmt.Printf("processed request %+v, sending response %+v\n", qr.req, resp)
			qr.conn.resp <- resp
		}
	}

}

func (ls *LeaseServer) createLease(r request, c *clientConn) (response, error) {
	// generate new lease
	ls.leaseIdCounter += 1
	leaseid := ls.leaseIdCounter
	l := lease{leaseid, r.Inodeid, c}
	// record lease in ls.inodeLeases under inode id
	leasesForInode, exists := ls.leasesByInode[r.Inodeid]
	if !exists {
		leasesForInode = make([]lease, 0, 0)
	}
	leasesForInode = append(leasesForInode, l)
	ls.leasesByInode[r.Inodeid] = leasesForInode
	// record in ls.leases under lease Id
	ls.leasesById[l.leaseid] = l
	return response{r.Reqno, leaseid, r.Inodeid, STATUS_OK, 0, 0}, nil
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
	resp := response{r.Reqno, 0, 0, STATUS_OK, 0, 0}
	return resp, nil
}

// release all belonging to a given client
func (ls *LeaseServer) releaseAll(c *clientConn) error {
	// iterate all leases
	for _, inodeLeases := range ls.leasesByInode {
		for idx, lease := range inodeLeases {
			//fmt.Printf("Checking if lease on inode %d belongs to our client :  %d == %d\n", lease.inodeid, lease.client.id, c.id)
			if lease.client.id == c.id {
				//fmt.Printf("Deleting\n")
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

func (ls *LeaseServer) notify(r request, c *clientConn) error {
	// find all readleases attached to this inode id
	readLeases := ls.leasesByInode[r.Inodeid]

	pendingAcks := make([]pendingAck, 0, len(readLeases)-1)
	// notify them all
	for _, rl := range readLeases {
		if rl.client.id != c.id {
			ls.ackIdCounter += 1
			pendingAcks = append(pendingAcks, rl.client.notify(r.Inodeid, rl.leaseid, ls.ackIdCounter, r.NotifyStartPos, r.NotifyLength))
		}
	}
	// spin off function which responds to committer after all readleases have acked
	go func() {
		// wait all readers acknowledged
		for _, ack := range pendingAcks {
			ack.waitAcknowledged()
		}
		// queue response to be sent to the client who committed
		c.resp <- response{r.Reqno, r.Leaseid, r.Inodeid, STATUS_OK, r.NotifyStartPos, r.NotifyLength}
	}()
	return nil
}

func (ls *LeaseServer) checkLeases(r request, c *clientConn) (response, error) {
	inodeLeases := ls.leasesByInode[r.Inodeid]
	//fmt.Printf("Checking leases for inode %d : %+v\n", r.Inodeid, inodeLeases)
	if inodeLeases != nil && len(inodeLeases) > 0 {
		return response{r.Reqno, r.Leaseid, r.Inodeid, STATUS_WAIT, 0, 0}, nil
	}
	return response{r.Reqno, r.Leaseid, r.Inodeid, STATUS_OK, 0, 0}, nil
}

// atomically adds incr to val, returns new val
func incrementAndGet(val *uint64, incr uint64) uint64 {
	currVal := atomic.LoadUint64(val)
	for !atomic.CompareAndSwapUint64(val, currVal, currVal+incr) {
		currVal = atomic.LoadUint64(val)
	}
	return currVal + incr
}
