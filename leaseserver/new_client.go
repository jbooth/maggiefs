package leaseserver

import (
	"encoding/gob"
	"fmt"
	"net"
)

type rawclient struct {
	c          *net.TCPConn
	reqcounter uint64
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

func newRawClient(addr string) (rawclient, error) {
	tcpAddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return rawclient{}, err
	}
	c, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return rawclient{}, err
	}
	c.SetNoDelay(true)
	c.SetKeepAlive(true)
	return rawclient{c, 0, make(chan uint64, 100), make(chan queuedRequest), make(chan response), make(chan bool)}, nil
}

func (c rawclient) doRequest(r request) (response, error) {
	respChan := make(chan response)
	q := queuedRequest{r, respChan}
	c.requests <- q
	resp := <-respChan
	return resp, nil
}

func (c rawclient) mux() {
	responseChans := make(map[uint64]chan response)
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
func (c rawclient) readResponses() {
	respDecoder := gob.NewDecoder(c.c)
	for {
    resp := response{}
    respDecoder.Decode(resp)
    c.responses <- resp  
	}
}
