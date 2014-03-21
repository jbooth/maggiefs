package leaseserver

import (
	"encoding/binary"
	"encoding/gob"
	"github.com/jbooth/maggiefs/maggiefs"
	"net"
)

type rawclient struct {
	id         uint64
	c          *net.TCPConn
	reqcounter uint64
	notifier   chan maggiefs.NotifyEvent
	requests   chan queuedRequest
	responses  chan response
	closeMux   chan bool
	//closeResponseReader chan bool
}

type queuedRequest struct {
	r        request
	whenDone chan response
}

func newRawClient(c *net.TCPConn) (*rawclient, error) {
	c.SetNoDelay(true)
	c.SetKeepAlive(true)
	idBuff := make([]byte, 8, 8)
	_, err := c.Read(idBuff)
	if err != nil {
		return nil, err
	}
	ret := &rawclient{binary.LittleEndian.Uint64(idBuff), c, 0, make(chan maggiefs.NotifyEvent, 100), make(chan queuedRequest), make(chan response), make(chan bool)}
	// read client id

	go ret.mux()
	go ret.readResponses()
	return ret, nil
}

// executes a request and blocks until response
// don't worry about the reqno field of request, that's handled internally
func (c *rawclient) doRequest(r request) (response, error) {
	respChan := make(chan response)
	q := queuedRequest{r, respChan}
	c.requests <- q
	resp := <-respChan
	return resp, nil
}

func (c *rawclient) sendRequestNoResponse(r request) {
	q := queuedRequest{r, nil}
	c.requests <- q
}

func (c *rawclient) mux() {
	responseChans := make(map[uint64]chan response)
	reqEncoder := gob.NewEncoder(c.c)
	for {
		select {
		case req := <-c.requests:
			// register response channel
			//fmt.Printf("storing respChan %+v under reqno %d\n",req.whenDone,req.r.Reqno)
			if req.whenDone != nil {
				c.reqcounter++
				req.r.Reqno = c.reqcounter
				responseChans[req.r.Reqno] = req.whenDone
			}
			// write the req to socket
			err := reqEncoder.Encode(req.r)
			if err != nil {
				continue
			}
		case resp := <-c.responses:
			if resp.Status == STATUS_NOTIFY {
				// this is a notification so forward to the notification chan
				c.notifier <- NotifyEvent{inodeid: resp.Inodeid, ackid: resp.Reqno, offset: resp.NotifyStartPos, length: resp.NotifyLength, c: c}
			} else {
				// response to a request, forward to it's response chan
				k := resp.Reqno
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
