package mrpc

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

const rpcHandlerId = uint32(0)

// connects to the RPC service on remote server
func DialRPC(addr *net.TCPAddr) (*rpc.Client, error) {
	conn, err := DialHandler(addr, rpcHandlerId)
	if err != nil {
		return nil, err
	}
	return rpc.NewClient(conn), nil
}

// connects to the indexed customHandler on remote server
func DialHandler(addr *net.TCPAddr, handler uint32) (*net.TCPConn, error) {
	handlerIdBytes := make([]byte, 4, 4)
	binary.LittleEndian.PutUint32(handlerIdBytes, handler)
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, fmt.Errorf("CloseableServer.DialHandler: Error connecting to host %s : %s", addr, err)
	}
	// route to handler and let it take over
	log.Printf("Dialing handler %d for conn %s", handler, conn)
	log.Printf("HandlerID bytes: %x", handlerIdBytes)
	_, err = conn.Write(handlerIdBytes)
	return conn, err
}

// wraps the provided impl for gob rpc, along with any custom handlers provided,
// while providing convenient Service interface
// custom handler funcs should execute perpetually and die without panicking, we spin them off in a goroutine as accepted
func CloseableRPC(listenAddr string, rpcServiceName string, rpcImpl interface{}, customHandlers map[uint32]func(newlyAcceptedConn *net.TCPConn)) (*CloseableServer, error) {
	listenTCPAddr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	for id, _ := range customHandlers {
		if id == rpcHandlerId {
			return nil, fmt.Errorf("One of the customHandlers was using the reserved handler ID %d", rpcHandlerId)
		}
	}
	rpcServer := rpc.NewServer()
	rpcServer.RegisterName(rpcServiceName, rpcImpl)
	customHandlers[rpcHandlerId] = func(conn *net.TCPConn) {
		buf := bufio.NewWriter(conn)
		codec := &gobServerCodec{conn, gob.NewDecoder(conn), gob.NewEncoder(buf), buf}
		rpcServer.ServeCodec(codec)
	}
	listener, err := net.ListenTCP("tcp", listenTCPAddr)
	if err != nil {
		return nil, err
	}
	log.Printf("CloseableServer opened listener on %s\n", listenAddr)
	ret := &CloseableServer{
		make(map[int]*net.TCPConn),
		listener,
		customHandlers,
		false,
		sync.NewCond(&sync.Mutex{}),
		new(sync.Mutex)}
	return ret, nil
}

type CloseableServer struct {
	conns    map[int]*net.TCPConn
	listen   *net.TCPListener
	handlers map[uint32]func(newlyAcceptedConn *net.TCPConn)
	closed   bool
	closeCnd *sync.Cond
	l        *sync.Mutex
}

func (r *CloseableServer) Addr() net.Addr {
	return r.listen.Addr()
}

// blocking accept loop
func (r *CloseableServer) Serve() error {
	sockIdCounter := 0
	log.Printf("Closable server listening on %s", r.listen)
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
		if r.closed { // double check after locking
			// close before shutting down
			return nil
		} else {
			// store reference and serve requests
			sockIdCounter += 1
			r.conns[sockIdCounter] = conn
			go func() {
				defer func() {
					if x := recover(); x != nil {
						fmt.Printf("run time panic for service on addr %s : %s\n", r.listen.Addr().String(), x)
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

func (r *CloseableServer) serveConn(conn *net.TCPConn) {
	log.Printf("CloseableServer got conn %s, reading handler ID", conn)
	handlerIdBytes := make([]byte, 4, 4)
	nRead := 0
	for nRead < 4 {
		n, err := conn.Read(handlerIdBytes[nRead:])
		if err != nil {
			log.Printf("Error reading initial routing for handler from CloseableServer.serveConn: %s : %s", conn, err)
			return
		}
		nRead += n
	}
	log.Printf("CloseableServer got handler bytes %x", handlerIdBytes)
	handlerId := binary.LittleEndian.Uint32(handlerIdBytes)
	log.Printf("CloseableServer got handler ID %d from conn %s", handlerId, conn)
	handler, ok := r.handlers[handlerId]
	if !ok {
		log.Printf("CloseableServer has no registered handler for id %d", handlerId)
		return
	}
	handler(conn)
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
		delete(r.conns, id)
	}
	r.closed = true
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
