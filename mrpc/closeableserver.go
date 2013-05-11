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
	fmt.Printf("listening on %s\n",listenAddr)
	rpcServer := rpc.NewServer()
	rpcServer.RegisterName(name,impl)
	onAccept := func(conn *net.TCPConn) {
		fmt.Println("accepted conn!")
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
	done        chan bool
	stopRequest bool
	l           *sync.Mutex
	onAccept    func(*net.TCPConn)
}

func NewCloseServer(listen *net.TCPListener, onAccept func(*net.TCPConn)) *CloseableServer {
	return &CloseableServer{make(map[int]*net.TCPConn), listen, make(chan bool), false, new(sync.Mutex), onAccept}
}

// spins off accept loop
func (r *CloseableServer) Start() {
	go r.Accept()
}

// blocking accept loop
func (r *CloseableServer) Accept() {
	for {
		fmt.Println("accepting")
		conn, err := r.listen.AcceptTCP()
		if err != nil {
			// stop accepting, shut down
			fmt.Printf("Error accepting, stopping acceptor: %s\n",err.Error())
			r.listen.Close()
			r.done <- true
			return
		}
		file, err := conn.File()
		if err != nil {
			// throw out connection
			conn.Close()
		} else {
			// going to store connection
			r.l.Lock()
			if r.stopRequest { // double check after locking
				// close before shutting down
				r.listen.Close()
				conn.Close()
				r.done <- true
				return
			} else {
				// store reference and serve requests
				r.conns[int(file.Fd())] = conn
				r.onAccept(conn)
			}
			r.l.Unlock()
		}
	}
}


func (r *CloseableServer) Close() error {
	r.l.Lock()
	defer r.l.Unlock()
	err := r.listen.Close()
	for _, conn := range r.conns {
		conn.Close()
	}
	return err
}

func (r *CloseableServer) WaitClosed() error {
	<-r.done
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
	if err != nil { fmt.Println(err.Error()) }
	return err
}

func (c *gobServerCodec) ReadRequestBody(body interface{}) error {
	err := c.dec.Decode(body)
	if err != nil { fmt.Println(err.Error()) }
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
