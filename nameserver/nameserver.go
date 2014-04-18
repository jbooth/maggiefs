package nameserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"net"
	"net/http"
	"os"
	"sync"
	"time"
)

// compile time typecheck
var typeCheck maggiefs.NameService = &NameServer{}

// new nameserver and lease server listening on the given addresses, serving data from dataDir
// addresses should be a 0.0.0.0:9999 type address
func NewNameServer(webAddr string, dataDir string, replicationFactor uint32, format bool) (*NameServer, error) {
	ns := &NameServer{}
	var err error = nil
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
	ns.webServer = newNameWebServer(ns, webAddr) // todo get rid of webserver and do this stuff via getattr
	return ns, nil
}

type NameServer struct {
	ls          maggiefs.LeaseService
	nd          *NameData
	rm          *replicationManager
	listenAddr  string
	dirTreeLock *sync.Mutex // used so all dir tree operations (link/unlink) are atomic
	webListen   net.Listener
	webServer   *http.Server
}

// func to serve web stuff
func (ns *NameServer) ServeWeb() error {
	errChan := make(chan error)
	go func() {
		defer func() {
			if x := recover(); x != nil {
				fmt.Printf("run time panic from nameserver web: %v\n", x)
				errChan <- fmt.Errorf("Run time panic: %v", x)
			}
		}()
		errChan <- ns.webServer.Serve(ns.webListen)
	}()
	err := <-errChan
	return err
}

func (ns *NameServer) Close() error {
	var retError error = nil
	err := ns.nd.Close()
	if err != nil {
		retError = err
	}
	err = ns.webListen.Close()
	if err != nil {
		retError = err
	}
	return retError
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

//func (ns *NameServer) SetInode(node *maggiefs.Inode) (err error) {
//	return ns.nd.SetInode(node)
//}

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
	// don't bother with this for now, screw posix
	//	err := ns.ls.WaitAllReleased(inodeid)
	//	if err != nil {
	//		fmt.Printf("error waiting all released for node %d : %s\n", inodeid, err.Error())
	//	}
	// truncating to 0 bytes will remove all blocks
	_, err := ns.Truncate(inodeid, 0)
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

func (ns *NameServer) Extend(nodeid uint64, newLen uint64) (newNode *maggiefs.Inode, err error) {
	// get current inode
	return ns.nd.Mutate(nodeid, func(i *maggiefs.Inode) error {
		i.Length = newLen
		return nil
	})
}

func (ns *NameServer) AddBlock(nodeid uint64, blockStartPos uint64, requestedDnId *uint32) (newNode *maggiefs.Inode, err error) {
	// lock on this inode so we don't add the same block twice
	added := false
	// check which hosts we want
	vols, err := ns.rm.volumesForNewBlock(nil)
	if err != nil {
		return nil, err
	}
	volIds := make([]uint32, len(vols))
	for idx, v := range vols {
		volIds[idx] = v.VolId
	}
	i, err := ns.nd.Mutate(nodeid, func(i *maggiefs.Inode) error {
		// make sure we can add this block
		if blockStartPos == 0 && len(i.Blocks) > 0 {
			return nil
		}
		// if we're adding a non-zero block
		if blockStartPos > 0 && len(i.Blocks) > 0 {
			lastBlock := i.Blocks[len(i.Blocks)-1]
			// if we don't need to add a new block, bail early
			if lastBlock.EndPos > blockStartPos {
				return nil
			}
			// make sure this wouldn't put a hole in the file
			if lastBlock.EndPos != (blockStartPos - 1) {
				return fmt.Errorf("Block end pos was %d, wanted %d : inode %+v", lastBlock.EndPos, blockStartPos-1, i)
			}
		}
		added = true
		newBlockId, err := ns.nd.GetIncrCounter(COUNTER_BLOCK, 1)
		if err != nil {
			return err
		}
		endPos := blockStartPos + maggiefs.BLOCKLENGTH - 1
		newBlock := maggiefs.Block{
			Id:       newBlockId,
			Inodeid:  i.Inodeid,
			Version:  0,
			StartPos: blockStartPos,
			EndPos:   endPos,
			Volumes:  volIds,
		}
		if i.Blocks == nil {
			i.Blocks = make([]maggiefs.Block, 0)
		}
		i.Blocks = append(i.Blocks, newBlock)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if added {
		// make sure DNs know about new block
		err = ns.rm.AddBlock(i.Blocks[len(i.Blocks)-1], false)
	}
	return i, err
}

func (ns *NameServer) Truncate(nodeid uint64, newSize uint64) (*maggiefs.Inode, error) {
	inode, err := ns.GetInode(nodeid)
	if err != nil {
		return nil, err
	}
	if len(inode.Blocks) > 0 {
		delset := make([]maggiefs.Block, 0, 0)
		var truncBlock *maggiefs.Block = nil
		var truncLength uint32 = 0
		// remove blocks from inode
		_, err = ns.nd.Mutate(inode.Inodeid, func(i *maggiefs.Inode) error {
			for idx := len(i.Blocks) - 1; idx >= 0; idx-- {
				blk := i.Blocks[idx]
				if blk.EndPos > newSize {
					// either delete or truncate
					if blk.StartPos >= newSize {
						// delete
						delset = append(delset, blk)
						i.Blocks = i.Blocks[:len(i.Blocks)-1]
					} else {
						// truncate
						truncLength = uint32(newSize - blk.StartPos)
						truncBlock = &blk
					}
				}
			}
			i.Length = newSize
			return nil
		})
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
		// remove blocks from datanodes
		for _, rmblk := range delset {
			err = ns.rm.RmBlock(rmblk)
			if err != nil {
				return nil, err
			}
		}
		if truncBlock != nil {
			err = ns.rm.TruncBlock(*truncBlock, truncLength)
		}

	}
	return inode, nil
}

func (ns *NameServer) Fallocate(nodeid uint64, newSize uint64, requestedDnId *uint32) error {
	//fmt.Printf("Adding/extending blocks for write at off %d length %d\n", off, length)
	ino, err := ns.GetInode(nodeid)
	if err != nil {
		return err
	}

	for newSize > ino.Length {
		newBlockPos := uint64(0)
		if len(ino.Blocks) > 0 {
			newBlockPos = ino.Blocks[len(ino.Blocks)-1].EndPos + 1
		}
		ino, err = ns.AddBlock(nodeid, newBlockPos, requestedDnId)
		if err != nil {
			return err
		}
		newLen := ino.Blocks[len(ino.Blocks)-1].EndPos - 1
		if newLen > newSize {
			newLen = newSize
		}
		ino, err = ns.Extend(nodeid, newSize)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ns *NameServer) SetAttr(nodeid uint64, input maggiefs.SetAttr) (inode *maggiefs.Inode, err error) {
	// if this is a truncate, handle the truncation separately from other mutations, it requires a special call
	// we don't handle size here, that should be forwatded to SetLength from the client side
	// other mutations, if applicable
	return ns.nd.Mutate(nodeid, func(inode *maggiefs.Inode) error {
		if input.SetMode {
			// chmod
			inode.Mode = uint32(07777) & input.Mode
		}
		if input.SetUid || input.SetGid {
			// chown
			inode.Uid = input.Uid
			inode.Gid = input.Gid
		}
		//if input.SetMtime {
		//	// MTIME, not supporting ATIME
		//	if input.Valid&fuse.FATTR_MTIME_NOW != 0 {
		//		inode.Mtime = time.Now().UnixNano()
		//	} else {
		//		inode.Mtime = int64(input.Mtime*1e9) + int64(input.Mtimensec)
		//	}
		//}
		return nil
	})
}

func (ns *NameServer) Join(dnId uint32, nameDataAddr string) (err error) {
	// TODO confirm not duplicate datanode
	fmt.Printf("Got connection from dn id %d, addr %s\n", dnId, nameDataAddr)
	raddr, err := net.ResolveTCPAddr("tcp", nameDataAddr)
	if err != nil {
		return err
	}

	cli, err := mrpc.DialRPC(raddr)
	if err != nil {
		err = fmt.Errorf("Error connecting to peer %d at %s : %s", dnId, nameDataAddr, err.Error())
		fmt.Println(err)
		return err
	}
	peer := maggiefs.NewPeerClient(cli)
	return ns.rm.addDn(peer)
}

func (ns *NameServer) NextVolId() (id uint32, err error) {
	ret, err := ns.nd.GetIncrCounter(COUNTER_VOLID, 1)
	return uint32(ret), err
}

func (ns *NameServer) NextDnId() (id uint32, err error) {
	ret, err := ns.nd.GetIncrCounter(COUNTER_DNID, 1)
	return uint32(ret), err
}
