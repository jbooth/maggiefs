package nameserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// new nameserver and lease server listening on the given addresses, serving data from dataDir
// addresses should be a 0.0.0.0:9999 type address
func NewNameServer(ls maggiefs.LeaseService, nameAddr string, webAddr string, dataDir string, replicationFactor uint32, format bool) (*NameServer, error) {
	ns := &NameServer{}
	var err error = nil
	ns.ls = ls
	ns.listenAddr = nameAddr
	if format {
		err = Format(dataDir, uint32(os.Getuid()), uint32(os.Getgid()))
		if err != nil {
			return nil, err
		}
	}
	ns.nd, err = NewNameData(dataDir)
	if err != nil {
		return nil, err
	}
	ns.rm = newReplicationManager(replicationFactor)
	ns.dirTreeLock = &sync.Mutex{}
	ns.webListen, err = net.Listen("tcp", webAddr)
	if err != nil {
		return nil, err
	}
	ns.webServer = newNameWebServer(ns, webAddr)
	ns.rpcServer, err = mrpc.CloseableRPC(ns.listenAddr, mrpc.NewNameServiceService(ns), "NameService")
	if err != nil {
		return nil,err
	}
	ns.clos = sync.NewCond(new(sync.Mutex))
	ns.closed = false
	return ns, nil
}

type NameServer struct {
	ls          maggiefs.LeaseService
	nd          *NameData
	rm          *replicationManager
	listenAddr  string
	dirTreeLock *sync.Mutex           // used so all dir tree operations (link/unlink) are atomic
	rpcServer   *mrpc.CloseableServer // created at start time, need to be closed
	webListen   net.Listener
	webServer   *http.Server
	clos *sync.Cond
	closed bool
	
}

func (ns *NameServer) Serve() error {
	var err error = nil
	errChan := make(chan error,3)
	go func() {
		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic from nameserver rpc: %v\n", x)
				errChan <- fmt.Errorf("Run time panic: %v", x)
			}
		}()
		errChan <- ns.rpcServer.Serve()
	}()

	go func() {
		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic from nameserver web: %v\n", x)
				errChan <- fmt.Errorf("Run time panic: %v", x)
			}
		}()
		errChan <- ns.webServer.Serve(ns.webListen)
	}()
	err = <- errChan
	return err
}

func (ns *NameServer) Close() error {
	ns.clos.L.Lock()
	defer ns.clos.L.Unlock()
	if ns.closed {
		return nil
	}
	var retError error = nil
	err := ns.nd.Close()
	if err != nil {
		retError = err
	}
	err = ns.rpcServer.Close()
	if err != nil {
		retError = err
	}
	err = ns.webListen.Close()
	if err != nil {
		retError = err
	}
	ns.closed = true
	ns.clos.Broadcast()
	return retError
}

func (ns *NameServer) WaitClosed() error {
	ns.clos.L.Lock()
	for ! ns.closed {
		ns.clos.Wait()
	}
	ns.clos.L.Unlock()
	return ns.rpcServer.WaitClosed()
}

func (ns *NameServer) HttpAddr() string {
	return ns.webServer.Addr
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
			//fmt.Printf("Checking if name %s exists on inode %+v, looking to link child %d\n",name,i,child)
			dentry, childExists := i.Children[name]
			if childExists {
				prevChildId = dentry.Inodeid
				//fmt.Printf("Link: other child exists for name %s, childID: %d\n",name,prevChildId)
				return nil
			} else {
				//fmt.Printf("Link: setting %d [%s] -> %d\n",i.Inodeid,name,child)
				now := time.Now().Unix()
				i.Children[name] = maggiefs.Dentry{child, now}
				i.Ctime = now
				parentSuccess = true
				prevChildId = 0
			}
			return nil
		})
		if err != nil {
			fmt.Printf("Link: returning  %d [%s] -> %d : %s\n", parent, name, child, err.Error())
			return fmt.Errorf("Link: returning  %d [%s] -> %d : %s", parent, name, child, err.Error())
		}
		//fmt.Printf("Link() checking if force situation, prevChildId: %d\n",prevChildId)
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
		fmt.Printf("error waiting all released for node %d : %s\n", inodeid, err.Error())
	}
	// truncating to 0 bytes will remove all blocks
	err = ns.Truncate(inodeid, 0)
	if err != nil {
		fmt.Printf("error truncating node %d : %s\n", inodeid, err.Error())
	}
	// now clean up the inode itself
	err = ns.nd.DelInode(inodeid)
	if err != nil {
		fmt.Printf("error deleting node %d from node store : %s\n", inodeid, err.Error())
	}
}

func (ns *NameServer) StatFs() (stat maggiefs.FsStat, err error) {
	return ns.rm.FsStat()
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

	endPos := startPos + uint64(length) - 1
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
	//fmt.Printf("nameserver addblock returning new block %+v\n",b)
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

		for idx := len(inode.Blocks) - 1; idx >= 0; idx-- {
			blk := inode.Blocks[idx]
			if blk.EndPos > newSize {
				// either delete or truncate
				if blk.StartPos >= newSize {
					// delete
					delset = append(delset, blk)
					inode.Blocks = inode.Blocks[:len(inode.Blocks)-1]
				} else {
					// truncate
					truncLength = uint32(newSize - blk.StartPos)
					truncBlock = &blk
				}
			}
		}
		for _, rmblk := range delset {
			err = ns.rm.RmBlock(rmblk)
			if err != nil {
				return err
			}
		}
		if truncBlock != nil {
			err = ns.rm.TruncBlock(*truncBlock, truncLength)
		}

	}
	inode.Length = newSize
	return ns.nd.SetInode(inode)
}

func (ns *NameServer) Join(dnId uint32, nameDataAddr string) (err error) {
	fmt.Printf("Got connection from dn id %d, addr %s\n", dnId, nameDataAddr)
	// TODO confirm not duplicate datanode
	client, err := rpc.Dial("tcp", nameDataAddr)
	if err != nil {
		err = fmt.Errorf("Error connecting to client %d at %s : %s", dnId, nameDataAddr, err.Error())
		fmt.Println(err)
		return err
	}
	nameData := mrpc.NewNameDataIfaceClient(client)
	return ns.rm.addDn(nameData)
}

func (ns *NameServer) NextVolId() (id uint32, err error) {
	ret, err := ns.nd.GetIncrCounter(COUNTER_VOLID, 1)
	return uint32(ret), err
}

func (ns *NameServer) NextDnId() (id uint32, err error) {
	ret, err := ns.nd.GetIncrCounter(COUNTER_DNID, 1)
	return uint32(ret), err
}
