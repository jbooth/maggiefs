package nameserver

import (
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"net"
	"time"
)

// we need to keep
//  leveldb of inodes
//  leveldb of blocks
//  global system statsfs

// additionally
//   connections to data servers
//   poll them to update stats

func NewNameServer(clPort int, dnPort int, dataDir string, format bool) (*NameServer, error) {
	ret := &NameServer{}
	clientListenAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("0.0.0.0:%d", clPort))
	if err != nil {
		ret.Close()
		return nil, err
	}
	ret.listenCL, err = net.ListenTCP("tcp", clientListenAddr)
	if err != nil {
		ret.Close()
		return nil, err
	}
	dnListenAddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("0.0.0.0:%d", dnPort))
	if err != nil {
		ret.Close()
		return nil, err
	}
	ret.listenDN, err = net.ListenTCP("tcp", dnListenAddr)
	if err != nil {
		ret.Close()
		return nil, err
	}
	ret.shutdownCL = make(chan bool, 1)
	ret.shutdownDN = make(chan bool, 1)
	ret.nd, err = NewNameData(dataDir)
	if err != nil {
		ret.Close()
		return nil, err
	}
	ret.rm = newReplicationManager()
	return ret, nil
}

type NameServer struct {
	rm         *replicationManager
	nd         *NameData
	listenCL   *net.TCPListener
	listenDN   *net.TCPListener
	conns      []*conn
	shutdownCL chan bool // these chans are triggered to stop accepting conns
	shutdownDN chan bool
}

func (ns *NameServer) acceptClients() {
	for {
		// accept (and listen for shutdown while accepting)
		cli, err := acceptOrShutdown(ns.listenCL, ns.shutdownCL)
		if err != nil {
			if err == e_shutdown {
				break
			} else {
				fmt.Printf("error accepting client : %s\n", err.Error())
				continue
			}
		}
		// serve
		c := conn{cli, gob.NewDecoder(cli), gob.NewEncoder(cli), ns}
		go c.serve()
	}
	// shutdown
	err := ns.listenCL.Close()
	if err != nil {
		fmt.Printf("error closing client listener : %s\n", err.Error())
	}
}

func (ns *NameServer) acceptDNs() {
	for {
		// select
		d, err := acceptOrShutdown(ns.listenDN, ns.shutdownDN)
		if err != nil {
			if err == e_shutdown {
				break
			} else {
				fmt.Printf("error accepting dn : %s\n", err.Error())
				continue
			}
		}
		// add to replication pool
		ns.rm.addDN(d)
	}
	// shutdown
	err := ns.listenDN.Close()
	if err != nil {
		fmt.Printf("error closing dn listener : %s\n", err.Error())
	}
}

// shuts down the nameserver
func (ns *NameServer) Close() error {
	// stop listening
  ns.shutdownCL <- true
  ns.shutdownDN <- true
	// stop serving connections
  for _,c := range ns.conns {
    if c != nil { c.cloose() }
  }
	// shutdown replication manager and leveldb

	return nil
}

var e_shutdown error = errors.New("MFS_SHUTDOWN")

const DEADLINE = 10 // 10 seconds
// attempts to accept from l and to receive from shutdown.  either returns new TCPConn connection or nil and E_SHUTDOWN
func acceptOrShutdown(l *net.TCPListener, shutdown chan bool) (*net.TCPConn, error) {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(20 * 1e6 * time.Nanosecond) // 20 ms
		timeout <- true
	}()
	select {
	case <-shutdown:
		return nil, e_shutdown
	case <-timeout:
		break
	}
	// now try to accept
	l.SetDeadline(time.Now().Add(DEADLINE * time.Second))
	ret, err := l.AcceptTCP()
	return ret, err
}

type conn struct {
	c  *net.TCPConn
	d  *gob.Decoder
	e  *gob.Encoder
	ns *NameServer
}

func (c *conn) cloose() {
	c.c.Close()
}

func (c *conn) serve() {
	req := request{}
	for {
		// read request
		c.d.Decode(&req)
		var err error = nil
		resp := response{STAT_OK, []byte{}}
		// handle

		switch req.Op {

		case OP_GETINODE:
			// resp is encoded inode
			resp.Body, err = c.ns.nd.GetInode(req.Inodeid)
		case OP_SETINODE:
			// resp is empty
			var success bool
			success, err = c.ns.nd.SetInode(req.Inodeid, req.Generation, req.Body)
			if !success {
				resp.Status = STAT_RETRY
			}
		case OP_ADDINODE:
			// resp is uint64 id
			inode := maggiefs.ToInode(req.Body)
			inode.Inodeid, err = c.ns.nd.AddInode(inode)
			resp.Body = make([]byte, 8)
			binary.LittleEndian.PutUint64(resp.Body, inode.Inodeid)
		case OP_LINK:
			// 2 scenarios here, forced and unforced
			// todo fill in error einval
			linkReq := toLinkReq(req.Body)
			var success bool = false
			for !success {
				resp.Status, err = c.ns.doLink(req.Inodeid, linkReq)
				if err != nil {
					resp.Status = STAT_ERR
					break
				}

				if resp.Status == STAT_E_EXISTS {
					if linkReq.Force {
						// unlink and loop around again
						err = c.ns.doUnLink(req.Inodeid, linkReq.Name)
					} else {
						// bail and return E_EXISTS
						break
					}
				} else {
					// done!
					resp.Status = STAT_OK
					success = true
				}
			}

		case OP_UNLINK:
			// child name is sent as body
			// todo fill in errors
			err = c.ns.doUnLink(req.Inodeid, string(req.Body))
			if err != nil {
				resp.Status = STAT_ERR
			} else {
				resp.Status = STAT_OK
			}
			// we send no body
		case OP_ADDBLOCK:
			// resp is new block

			// find block locations
			// notify each to allocate

			// add to inode

			// return new block
		case OP_EXTENDBLOCK:
		// notify each DN location
		// modify on inode, update inode size
		// return
		default:
			err = fmt.Errorf("unrecognized op: %d", req.Op)
		}

		// send response
		if err != nil {
			resp.Status = STAT_ERR
			resp.Body = []byte(err.Error())
			fmt.Printf("error handling req %+v : %s", req, err.Error())
		}
		err = c.e.Encode(resp)
		if err != nil {
			fmt.Printf("Error sending response %+v : %s", resp, err)
		}
	}
}

// mutate an inode -- encoded as in binary.Read, binary.Write
func (ns *NameServer) mutate(inode uint64, mutator func(*maggiefs.Inode) (i *maggiefs.Inode, stat byte, e error)) (*maggiefs.Inode, byte, error) {
	success := false
	var i *maggiefs.Inode
	for !success {
		bytes, err := ns.nd.GetInode(inode)
		if err != nil {
			return nil, STAT_ERR, err
		}
		i = maggiefs.ToInode(bytes)
		lastGen := i.Generation
		i, stat, err := mutator(i)
		if err != nil || stat != STAT_OK {
			return i, stat, err
		}
		success, err = ns.nd.SetInode(inode, lastGen, maggiefs.FromInode(i))
		if err != nil {
			return nil, STAT_ERR, err
		}
	}
	return i, STAT_OK, nil
}

func (ns *NameServer) doLink(parentId uint64, linkReq linkReqBody) (status byte, err error) {
	// add link to parent, returning STAT_E_EXISTS if necessary
	_, status, err = ns.mutate(parentId,
		func(parent *maggiefs.Inode) (*maggiefs.Inode, byte, error) {

			_, childExists := parent.Children[linkReq.Name]
			if childExists {
				return nil, STAT_E_EXISTS, nil
			}
			parent.Children[linkReq.Name] = maggiefs.Dentry{linkReq.ChildId, time.Now().Unix()}
			return parent, STAT_OK, nil
		})
	if status != STAT_OK || err != nil {
		return status, err
	}

	// add Nlink to child
	_, status, err = ns.mutate(linkReq.ChildId, func(child *maggiefs.Inode) (*maggiefs.Inode, byte, error) {
		child.Nlink++
		return child, STAT_OK, nil
	})
	return status, err
}

func (ns *NameServer) doUnLink(parentId uint64, name string) error {
	// loop until parent updated
	success := false
	var childId uint64
	for !success {
		parentBytes, err := ns.nd.GetInode(parentId)
		if err != nil {
			return err
		}
		parent := maggiefs.ToInode(parentBytes)
		child, exists := parent.Children[name]
		if !exists {
			return fmt.Errorf("no child for inode %d with name %s", parentId, name)
		}
		childId = child.Inodeid
		delete(parent.Children, name)
		success, err = ns.nd.SetInode(parentId, parent.Generation, maggiefs.FromInode(parent))
		if err != nil {
			return err
		}
	}
	// loop until child nlinks updated
	success = false
	childNeedsGC := false
	for !success {
		childBytes, err := ns.nd.GetInode(childId)
		if err != nil {
			return err
		}
		child := maggiefs.ToInode(childBytes)
		child.Nlink--
		success, err = ns.nd.SetInode(childId, child.Generation, maggiefs.FromInode(child))
		if err != nil {
			return err
		}
		if success && child.Nlink == 0 {
			childNeedsGC = true
		}
	}
	if childNeedsGC {
	  // hint to GC
		fmt.Println("child should be GC'd, should hint to GC here")
	}
	return nil
}

func (ns *NameServer) addBlock(i *maggiefs.Inode, size uint64) *maggiefs.Block {
  return nil
  
  // check which hosts we want
 
  // store block info in NameData
  
  // replicate block to datanodes
}

