package leaseserver

import (
	"encoding/gob"
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
	ret := &rawclient{c, 0, make(chan uint64, 100), make(chan queuedRequest), make(chan response), make(chan bool)}
	go ret.mux()
	go ret.readResponses()
	return ret, nil
}

// executes a request and blocks until response
// don't worry about the reqno field of request, that's handled internally
func (c *rawclient) doRequest(r request) (response, error) {
	respChan := make(chan response)
	q := queuedRequest{r, respChan}
	fmt.Println("queueing req")
	c.requests <- q
	fmt.Printf("queued, awaiting resp on %+v\n",respChan)
	resp := <-respChan
	return resp, nil
}

func (c *rawclient) mux() {
	responseChans := make(map[uint32]chan response)
	reqEncoder := gob.NewEncoder(c.c)
	for {
		select {
		case req := <-c.requests:
		  fmt.Printf("client got req %+v, sending\n",req)
			// register response channel
			c.reqcounter++
			req.r.Reqno = c.reqcounter
			fmt.Printf("storing respChan %+v under reqno %d\n",req.whenDone,req.r.Reqno)
			responseChans[req.r.Reqno] = req.whenDone
			// write the req to socket
			err := reqEncoder.Encode(req.r)
			if err != nil {
				fmt.Println("error encoding req %+v : %s", req, err.Error())
				continue
			}
		case resp := <-c.responses:
		  fmt.Printf("client got resp %+v\n",resp)
			if resp.Status == STATUS_NOTIFY {
			  // this is a notification so forward to the notification chan
			  fmt.Printf("Got commit notification for inode %d",resp.Inodeid)
			  c.notifier <- resp.Inodeid  
			} else {
				// response to a request, forward to it's response chan
				k := resp.Reqno
				respChan := responseChans[k]
				delete(responseChans, k)
				fmt.Printf("sending response %+v to %+v\n",resp,respChan)
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
    fmt.Printf("client read response %+v from sock \n",resp)
    c.responses <- resp  
	}
}



