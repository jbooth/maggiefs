package mrpc

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

// wraps the provided impl for gob rpc
func CloseableRPC(listenAddr string, impl interface{}, name string) (*CloseableServer, error) {
	listenTCPAddr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", listenTCPAddr)
	if err != nil {
		return nil, err
	}
	fmt.Printf("listening on %s\n", listenAddr)
	rpcServer := rpc.NewServer()
	rpcServer.RegisterName(name, impl)
	onAccept := func(conn *net.TCPConn) {
		buf := bufio.NewWriter(conn)
		codec := &gobServerCodec{conn, gob.NewDecoder(conn), gob.NewEncoder(buf), buf}
		go rpcServer.ServeCodec(codec)
	}
	ret := NewCloseServer(listener, onAccept)
	return ret, nil
}

type CloseableServer struct {
	conns       map[int]*net.TCPConn
	listen      *net.TCPListener
	stopRequest bool
	closed		bool
	closeCnd    *sync.Cond
	l           *sync.Mutex
	serveConn    func(*net.TCPConn)
}

// A closeable Server wraps a tcpListener and a serveConn function and spins off the accept loop while
// implementing Service reliably.
func NewCloseServer(listen *net.TCPListener, serveConn func(*net.TCPConn)) *CloseableServer {
	return &CloseableServer{make(map[int]*net.TCPConn), listen, false, false, new(sync.Cond), new(sync.Mutex), serveConn}
}

// blocking accept loop
func (r *CloseableServer) Serve() error {
	sockIdCounter := 0
	for {
		conn, err := r.listen.AcceptTCP()
		if err != nil {
			// stop accepting, shut down
			fmt.Printf("Error accepting, stopping acceptor: %s\n", err.Error())
			r.Close()
			return err
		}
		conn.SetNoDelay(true)
		r.l.Lock()
		if r.stopRequest { // double check after locking
			// close before shutting down
			conn.Close()
			r.Close()
			return nil
		} else {
			// store reference and serve requests
			sockIdCounter += 1
			r.conns[sockIdCounter] = conn
			go func() {
				defer func() {
					if x := recover(); x != nil {
						fmt.Printf("run time panic for service on addr %s : %s\n", r.listen.Addr().String(),x)
						conn.Close()
						r.Close()
					}
				}()
				r.serveConn(conn)
			}()
		}
		r.l.Unlock()
	}
}

func (r *CloseableServer) Close() error {
	r.l.Lock()
	defer r.l.Unlock()
	err := r.listen.Close()
	for id, conn := range r.conns {
		err1 := conn.Close()
		if err1 != nil && err == nil {
			err = err1
		}
		delete(r.conns,id)
	}
	r.closeCnd.Broadcast()
	return err
}

func (r *CloseableServer) WaitClosed() error {
	r.closeCnd.L.Lock()
	for !r.closed {
		r.closeCnd.Wait()
	}
	r.closeCnd.L.Unlock()
	return nil
}

type gobServerCodec struct {
	rwc    *net.TCPConn
	dec    *gob.Decoder
	enc    *gob.Encoder
	encBuf *bufio.Writer
}

func (c *gobServerCodec) ReadRequestHeader(r *rpc.Request) error {
	err := c.dec.Decode(r)
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}

func (c *gobServerCodec) ReadRequestBody(body interface{}) error {
	err := c.dec.Decode(body)
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}
func (c *gobServerCodec) WriteResponse(r *rpc.Response, body interface{}) (err error) {
	if err = c.enc.Encode(r); err != nil {
		return
	}
	if err = c.enc.Encode(body); err != nil {
		return
	}
	return c.encBuf.Flush()
}
func (c *gobServerCodec) Close() error {
	return c.rwc.Close()
}
