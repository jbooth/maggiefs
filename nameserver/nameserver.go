package nameserver

import (
	"encoding/gob"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"net"
	"sync"
	"time"
	"sort"
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

type datanodeStat struct {
	conn maggiefs.NameDataIface
	stat maggiefs.DataNodeStat
	l    *sync.Mutex
}

type dnFreeSize struct {
  dn uint32
  freeSpace uint64
}

type dnFreeSlice []dnFreeSize
func (s dnFreeSlice) Swap(i,j int) { s[i],s[j] = s[j],s[i]}
func (s dnFreeSlice) Len() int  { return len(s) } 
func (s dnFreeSlice) Less(i,j int) bool { return s[i].freeSpace < s[j].freeSpace }

// returns a slice of hosts for a new block, should be ns.replicationFactor in length
// finds N with most space
func (ns *NameServer) HostsForNewBlock() ([]uint32,error) {
  ns.dnLock.Lock()
  defer ns.dnLock.Unlock()
  if ns.replicationFactor > len(ns.dataNodes) {
    return []uint32{},fmt.Errorf("Replication factor %d greater than number of connected data nodes %d : %+v",ns.replicationFactor,len(ns.dataNodes),ns.dataNodes)
  }
  // build slice
  var freeSizes dnFreeSlice = make([]dnFreeSize,len(ns.dataNodes))
  i := 0
  for id,dn := range ns.dataNodes {
    freeSizes[i] = dnFreeSize {id,dn.stat.TotalBytes - dn.stat.BytesUsed }
  }
  // sort
  sort.Sort(freeSizes)
  // return first N
  ret := make([]uint32,ns.replicationFactor)
  for i := 0 ; i < ns.replicationFactor ; i++ {
    ret[i] = freeSizes[i].dn
  }
  // todo should provisionally += the affected datanodes so we don't thundering-herd them
	return ret,nil
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
			linkReq := toLinkReq(req.Body)
			var success bool = false
			for !success {
				resp.Status, err = c.ns.doLink(req.Inodeid, linkReq)
				if err != nil {
					resp.Status = STAT_ERR
					break
				}

				if resp.Status == STAT_E_EXISTS && linkReq.Force {
					// unlink and loop around again
					err = c.ns.doUnLink(req.Inodeid, linkReq.Name)
				} else {
					// done!
					success = true
				}
			}

		case OP_UNLINK:
			// child name is crammed into body
			err = c.ns.doUnLink(req.Inodeid, string(req.Body))
			if err != nil {
				resp.Status = STAT_ERR
			}
		case OP_ADDBLOCK:

		case OP_EXTENDBLOCK:

			// unlink
			// addblock
			// extendblock
		}

		// send response
		if err != nil {
			resp.Status = STAT_ERR
			resp.Body = []byte(err.Error())
			fmt.Printf("error handling req %+v : %s", req, err)
		}
		err = c.e.Encode(resp)
		if err != nil {
			fmt.Printf("Error sending response %+v : %s", resp, err)
		}
	}
}

func (ns *NameServer) doLink(parentId uint64, linkReq linkReqBody) (status byte, err error) {
	// loop until parent updated
	success := false
	for !success {
		parentBytes, err := ns.nd.GetInode(parentId)
		if err != nil {
			return STAT_ERR, err
		}
		parent := toInode(parentBytes)
		_, childExists := parent.Children[linkReq.Name]
		if childExists {
			return STAT_E_EXISTS, nil
		}
		parent.Children[linkReq.Name] = maggiefs.Dentry{linkReq.ChildId, time.Now().Unix()}
		success, err = ns.nd.SetInode(parentId, parent.Generation, fromInode(parent))
		if err != nil {
			return STAT_ERR, err
		}
	}
	// loop until child nlinks updated
	success = false
	for !success {
		childBytes, err := ns.nd.GetInode(linkReq.ChildId)
		if err != nil {
			return STAT_ERR, err
		}
		child := toInode(childBytes)
		child.Nlink++
		success, err = ns.nd.SetInode(linkReq.ChildId, child.Generation, fromInode(child))
		if err != nil {
			return STAT_ERR, err
		}
	}
	return STAT_OK, nil
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
