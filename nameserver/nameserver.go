package nameserver

import (
	"encoding/gob"
	"encoding/binary"
	"bytes"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"net"
	"sync"
	"time"
	
)

// we need to keep
//  leveldb of inodes
//  leveldb of blocks
//  global system statsfs

// additionally
//   connections to data servers
//   poll them to update stats

type NameServer struct {
	replicationFactor int
	dataNodes         map[uint32]datanodeStat
	dnLock            *sync.Mutex
	nd                *NameData
	l                 *net.TCPListener
}



type conn struct {
	c  *net.TCPConn
	d  *gob.Decoder
	e  *gob.Encoder
	ns *NameServer
}

func (c conn) serve() {
	req := request{}
	for {
		// read request
		c.d.Decode(&req)
		var err error = nil
		resp := response{STAT_OK, []byte{}}
		// handle

		switch req.Op {

		case OP_GETINODE:
			resp.Body, err = c.ns.nd.GetInode(req.Inodeid)
		case OP_SETINODE:
			var success bool
			success, err = c.ns.nd.SetInode(req.Inodeid, req.Generation, req.Body)
			if !success {
				resp.Status = STAT_RETRY
			}
		case OP_LINK:
		  // 2 scenarios here, forced and unforced
			linkReq := toLinkReq(req.Body)
			var success bool = false
			for !success {
				resp.Status, err = c.ns.doLink(req.Inodeid, linkReq)
				if err != nil {
					resp.Status = STAT_ERR
					break
				}

				if resp.Status == STAT_E_EXISTS {
				  if (linkReq.Force) {
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
			err = c.ns.doUnLink(req.Inodeid, string(req.Body))
			if err != nil {
				resp.Status = STAT_ERR
			} else {
			  resp.Status = STAT_OK
			}
			// we send no body
		case OP_ADDBLOCK:
      // find block locations
      // notify each to allocate
      
      // add to inode
      
      // return new block
		case OP_EXTENDBLOCK:
      // notify each DN location
      // modify on inode, update inode size
      // return
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

func fromInode(i *maggiefs.Inode) []byte {
  if i == nil { return []byte{} }
  ret := make([]byte,binary.Size(*i))
  binary.Write(bytes.NewBuffer(ret),binary.LittleEndian,i)
  return ret
}


func toInode(b []byte) *maggiefs.Inode {
  ret := &maggiefs.Inode{}
  binary.Read(bytes.NewBuffer(b),binary.LittleEndian,ret)
  return ret
}

// mutate an inode -- encoded as in binary.Read, binary.Write
func (ns *NameServer) mutate(inode uint64, mutator func(*maggiefs.Inode) (i *maggiefs.Inode,stat byte,e error)) (*maggiefs.Inode,byte,error) {
  success := false
  var i *maggiefs.Inode
  for !success{
    bytes,err := ns.nd.GetInode(inode)
    if err != nil {
      return nil,STAT_ERR,err
    }
    i = toInode(bytes)
    lastGen := i.Generation
    i,stat,err := mutator(i)
    if err != nil || stat != STAT_OK {
      return i,stat,err
    }
    success,err = ns.nd.SetInode(inode,lastGen,fromInode(i))
    if err != nil {
      return nil,STAT_ERR,err
    }
  }
  return i,STAT_OK,nil
}


func (ns *NameServer) doLink(parentId uint64, linkReq linkReqBody) (status byte, err error) {
  // add link to parent, returning STAT_E_EXISTS if necessary
  _,status,err = ns.mutate(parentId,
    func(parent *maggiefs.Inode) (*maggiefs.Inode,byte,error) {
    
      _, childExists := parent.Children[linkReq.Name]
      if childExists {
        return nil,STAT_E_EXISTS, nil
      }
      parent.Children[linkReq.Name] = maggiefs.Dentry{linkReq.ChildId, time.Now().Unix()}
      return parent,STAT_OK,nil
    })
  if status != STAT_OK || err != nil {
    return status,err
  }
  
  // add Nlink to child
  _,status,err = ns.mutate( linkReq.ChildId,func(child *maggiefs.Inode) (*maggiefs.Inode,byte,error) {
    child.Nlink++
    return child,STAT_OK,nil
  })
  return status,err
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
		parent := toInode(parentBytes)
		child, exists := parent.Children[name]
		if !exists {
			return fmt.Errorf("no child for inode %d with name %s", parentId, name)
		}
		childId = child.Inodeid
		delete(parent.Children, name)
		success, err = ns.nd.SetInode(parentId, parent.Generation, fromInode(parent))
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
		child := toInode(childBytes)
		child.Nlink--
		success, err = ns.nd.SetInode(childId, child.Generation, fromInode(child))
		if err != nil {
			return err
		}
		if success && child.Nlink == 0 {
			childNeedsGC = true
		}
	}
	if childNeedsGC {
		fmt.Println("child should be GC'd lol")
	}
	return nil
}

// goroutine methods
func (ns *NameServer) accept() {
	for {
		c, err := ns.l.AcceptTCP()
		if err != nil {
			fmt.Printf("Error accepting!! %s\nExiting.\n", err)
			panic(err)
		}
		conn := conn{
			c,
			gob.NewDecoder(c),
			gob.NewEncoder(c),
			ns,
		}
		go conn.serve()
	}

}

func (ns *NameServer) acceptDN() {
}

func (ns *NameServer) heartBeats() {
}
