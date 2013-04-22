package nameserver

import (
	"github.com/jbooth/maggiefs/leaseserver"
	"github.com/jbooth/maggiefs/maggiefs"
	"net"
	"net/rpc"
	"sync"
	"time"
	"fmt"
)

// new nameserver and lease server listening on the given addresses, serving data from dataDir
// addresses should be a 0.0.0.0:9999 type address
func NewNameServer(leaseServerAddr string, nameAddr string, dataDir string, format bool) (*NameServer, error) {
	ret := &NameServer{}
	leaseServer, err := leaseserver.NewLeaseServer(leaseServerAddr)
	if err != nil {
		return nil, err
	}
	go leaseServer.Serve()

	ret.nd, err = NewNameData(dataDir)
	if err != nil {
		return nil, err
	}

	ret.rm = newReplicationManager()
	clientListenAddr, err := net.ResolveTCPAddr("tcp", nameAddr)
	if err != nil {
		return nil, err
	}
	clientListen, err := net.ListenTCP("tcp", clientListenAddr)
	if err != nil {
		return nil, err
	}
	nameServerServer := maggiefs.NewNameServiceService(ret)
	server := rpc.NewServer()
	server.Register(nameServerServer)
	go server.Accept(clientListen)
	return ret, nil
}

type NameServer struct {
	ls          maggiefs.LeaseService
	rm          *replicationManager
	nd          *NameData
	listen      *net.TCPListener
	dirTreeLock *sync.Mutex // used so all dir tree operations (link/unlink) are atomic
}

func (ns *NameServer) GetInode(nodeid uint64) (node *maggiefs.Inode, err error) {
	return ns.nd.GetInode(nodeid)
}

func (ns *NameServer) AddInode(node *maggiefs.Inode) (id uint64, err error) {
	return ns.nd.AddInode(node)
}

func (ns *NameServer) SetInode(node *maggiefs.Inode) (err error) {
	return ns.nd.SetInode(node)
}

func (ns *NameServer) Link(parent uint64, child uint64, name string, force bool) (err error) {
	ns.dirTreeLock.Lock()
	defer ns.dirTreeLock.Unlock()
	var parentSuccess = false
	var prevChildId = uint64(0)
	for !parentSuccess {
		// try to link to parent
		_, err := ns.nd.Mutate(parent, func(i *maggiefs.Inode) error {
			dentry, childExists := i.Children[name]
			if childExists {
				prevChildId = dentry.Inodeid
				return nil
			} else {
				now := time.Now().Unix()
				i.Children[name] = maggiefs.Dentry{child, now}
				i.Ctime = now
				parentSuccess = true
				prevChildId = 0
			}
			return nil
		})
		if err != nil {
			return err
		}
		// if name already exists, handle
		if prevChildId > 0 {
			if force {
				err = ns.doUnlink(parent, name)
				if err != nil {
					return err
				}
			} else {
				return maggiefs.E_EXISTS
			}
		}
	}
	// now increment numLinks on child
	_, err = ns.nd.Mutate(child, func(i *maggiefs.Inode) error {
		i.Nlink++
		i.Ctime = time.Now().Unix()
		return nil
	})
	return err
}

func (ns *NameServer) Unlink(parent uint64, name string) (err error) {
	ns.dirTreeLock.Lock()
	defer ns.dirTreeLock.Unlock()
	return ns.doUnlink(parent, name)
}

// separate so we can call from Link with force
func (ns *NameServer) doUnlink(parent uint64, name string) (err error) {
	var childId = uint64(0)
	// unlink from parent
	_, err = ns.nd.Mutate(parent, func(i *maggiefs.Inode) error {
		dentry, exists := i.Children[name]
		if !exists {
			return maggiefs.E_NOENT
		}
		childId = dentry.Inodeid
		delete(i.Children, name)
		i.Ctime = time.Now().Unix()
		return nil
	})
	if err != nil {
		return nil
	}
	// decrement numlinks on child
	child, err := ns.nd.Mutate(childId, func(i *maggiefs.Inode) error {
		i.Nlink--
		i.Ctime = time.Now().Unix()
		return nil
	})
	if err != nil {
		return err
	}
	// garbage collect if necessary
	if child.Nlink <= 0 {
		go ns.del(child.Inodeid) 
	}
	return nil
}

// called to clean up an inode after it's been unlinked all the way
// invoked as a goroutine typically, 
func (ns *NameServer) del(inodeid uint64) {
	// wait until no clients have this file open (posix convention)
	err := ns.ls.WaitAllReleased(inodeid)
	if err != nil {
		fmt.Printf("error waiting all released for node %d : %s\n",inodeid,err.Error())
	}
	// truncating to 0 bytes will remove all blocks
	err = ns.Truncate(inodeid,0)
	if err != nil {
		fmt.Printf("error truncating node %d : %s\n",inodeid,err.Error())
	}
	// now clean up the inode itself
	err = ns.nd.DelInode(inodeid)
	if err != nil {
		fmt.Printf("error deleting node %d from node store : %s\n",inodeid,err.Error())
	}
}

func (ns *NameServer) StatFs() (stat maggiefs.FsStat, err error) {

	return maggiefs.FsStat{}, nil
}

func (ns *NameServer) AddBlock(nodeid uint64, length uint32) (newBlock maggiefs.Block, err error) {
	i, err := ns.nd.GetInode(nodeid)
	if err != nil {
		return maggiefs.Block{}, nil
	}
	// check which hosts we want
	vols, err := ns.rm.volumesForNewBlock(nil)
	if err != nil {
		return maggiefs.Block{}, err
	}
	volIds := make([]uint32, len(vols))
	for idx, v := range vols {
		volIds[idx] = v.VolId
	}

	var startPos uint64 = 0
	if len(i.Blocks) > 0 {
		lastBlock := i.Blocks[len(i.Blocks)-1]
		startPos = lastBlock.EndPos + 1
	} else {
		startPos = 0
	}

	endPos := startPos + uint64(length)
	// allocate block and id
	b := maggiefs.Block{
		Id:       0,
		Inodeid:  i.Inodeid,
		Version:  0,
		StartPos: startPos,
		EndPos:   endPos,
		Volumes:  volIds,
	}
	newId, err := ns.nd.AddBlock(b, i.Inodeid)
	b.Id = newId
	if err != nil {
		return maggiefs.Block{}, err
	}

	// replicate block to datanodes
	err = ns.rm.AddBlock(b)
	return b, err
}

func (ns *NameServer) Truncate(nodeid uint64, newSize uint64) (err error) {

	inode, err := ns.nd.GetInode(nodeid)
	if len(inode.Blocks) > 0 {
		delset := make([]maggiefs.Block, 0, 0)
		var truncBlock *maggiefs.Block = nil
		var truncLength uint32 = 0
		for idx := len(inode.Blocks); idx >= 0; idx-- {
			blk := inode.Blocks[idx]
			if blk.EndPos > newSize {
				// either delete or truncate
				if blk.StartPos >= newSize {
					// delete
					delset = append(delset, blk)
					inode.Blocks = inode.Blocks[:len(inode.Blocks) - 1]
				} else {
					// truncate
					truncLength = uint32(newSize - blk.StartPos)
					truncBlock = &blk
				}
			}
		}
		for _,rmblk := range delset {
			err = ns.rm.RmBlock(rmblk)
			if err != nil { return err }
		}
		if truncBlock != nil {
			err = ns.rm.TruncBlock(*truncBlock,truncLength)
		}
		
	}
	return nil
}

func (ns *NameServer) Join(dnId uint32, nameDataAddr string) (err error) {
	// TODO confirm not duplicate datanode
	client, err := rpc.Dial("tcp", nameDataAddr)
	nameData := maggiefs.NewNameDataIfaceClient(client)
	return ns.rm.addDn(nameData)
}

func (ns *NameServer) NextVolId() (id uint32, err error) {
	panic("unimplemented!")
	return 0, nil
}

func (ns *NameServer) NextDnId() (id uint32, err error) {
	panic("unimplemented!")
	return 0, nil
}
