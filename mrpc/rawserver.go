package mrpc

import (
	"net"
	"sync"
	"encoding/binary"
)

// wraps the provided impl for gob rpc
func NewRawServer(listenAddr string, ops map[byte]func([][]byte,*RemoteClient), name string) (*CloseableServer, error) {
	listenTCPAddr, err := net.ResolveTCPAddr("tcp", listenAddr)
	if err != nil {
		return nil, err
	}
	listener, err := net.ListenTCP("tcp", listenTCPAddr)
	if err != nil {
		return nil, err
	}
	fmt.Printf("listening on %s\n", listenAddr)
	onAccept := func(conn *net.TCPConn) {
		cli := remoteClient{
			Conn:   conn,
			Lock:   new(sync.Mutex)
		}
		for {
			// TODO we could recycle these buffers by allocating say 512kb per client at connect time and re-using
			// read header
			headerBytes := make([]byte,13,13)
			_,err := cli.Conn.Read(headerBytes)
			if err != nil {
				fmt.Printf("Error reading header! %s\n",err)
			}
			header := ReqHeader{}
			header.Opcode = headerBytes[0]
			header.Reqno = binary.LittleEndian.Uint64(headerBytes[1:])
			header.Numargs = binary.LittleEndian.Uint32(headerBytes[9:])
			// read args
			argLenBytes := make([]byte,header.Numargs*4,header.Numargs*4)
			_,err := cli.Conn.Read(argLenBytes)
			argLengths := make([]uint32,header.Numargs,header.Numargs)
			totalArgsLen := 0
			for i := 0 ; i < header.Numargs ; i++ {
				argLengths[i] = binary.LittleEndian.Uint32(argLenBytes[i*4:])
				totalArgsLen += argLengths[i]
			}
			flatArgs := make([]byte, totalArgsLen, totalArgsLen)
			_,err := cli.Conn.Read(flatArgs)
			args := make([][]byte,header.Numargs,header.Numargs)
			idx := 0
			for i := 0 ; i < len(argLengths) ; i++ {
				args[i] = flatArgs[idx:idx+argLengths[i]]
				idx += argLengths[i]
			}
			// dispatch op
			ops[header.Opcode](args,cli)
		}

	}
	ret := NewCloseServer(listener, onAccept)
	return ret, nil
}

type RemoteClient struct {
	Conn *net.TCPConn
	Lock *sync.Mutex
}
