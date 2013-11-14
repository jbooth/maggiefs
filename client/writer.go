package client

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"time"
)

type InodeWriter struct {
	// instantiated vars
	inodeid   uint64
	leases    maggiefs.LeaseService
	names     maggiefs.NameService
	datas     maggiefs.DataService
	localDnId *uint32

	// used to manage work queue
	workQueue chan *writeOp // 16 long for total of 2MB in flight at a time
}

type writeOp struct {
	data        []byte
	startPos    uint64
	length      uint32
	doTrunc     bool
	truncLength uint64
	doSync      bool
	doneChan    chan bool
}

func NewInodeWriter(inodeid uint64, leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService, localDnId *uint32) (w *InodeWriter, err error) {
	if err != nil {
		return nil, err
	}

	ret := &InodeWriter{inodeid, leases, names, datas, localDnId, make(chan *writeOp, 16), nil, nil}
	go ret.process()
	return ret, nil
}

func (w *InodeWriter) process() {
	for {
		// pull ops from the workqueue
		op, ok := <-w.workQueue
		if !ok {
			// close 
			return
		}
		var err error
		if op.doTrunc {

		}
		needLeaseForOp := false
		// if we're appending, we need a lease
		if op.data != nil {
			inoForLenCheck,err :=  w.names.GetInode(w.inodeid)
			if err != nil {
				fmt.Printf("Error getting ino %d : %s\n",w.inodeid,err)
			}
			if inoForLenCheck.Length < op.startPos + uint64(op.length) {
				// appending to file so we are modifying inode structure
				needLeaseForOp = true
			}
		}
		if op.doTrunc {
			// trunc needs lease becuase we're modifying inode structure
			needLeaseForOp = true 
		}
		// if we're getting an expensive lease, we'll try to hold it and coalesce a few writes
		if needLeaseForOp {
			
		} else {
			// didn't need a lease for this op, so just handle without
			if op.data != nil {
				
			}
			
		}
		if op.doneChan != nil {
			// inform blocking ops that they're done
			op.doneChan <- true
		}
	} // endfor
}

// acquires lease, does firstOp, then does up to numExtra more operations while holding lease
// this allows us to coalesce metadata updates and only sync to the cluster once
func (w *InodeWriter) doOpsWithLease(firstOp *writeOp, numExtra int) (closeReceived bool, err error) {
	// need to ensure lease for these ops
			l,err := w.leases.WriteLease(w.inodeid)
			if err != nil {
				fmt.Printf("Writer for inode %d process() couldn't acquire writeLease, quitting..\n", w.inodeid)
				return
			}
			// refresh local view of inode now that we have lease
			inode,err := w.names.GetInode(w.inodeid)
			if err != nil {
				fmt.Printf("Error getting ino %d : %s\n",w.inodeid,err)
				l.Release()
				return
			}
			// do first op
			doWriteOrTrunc(op)
			
			
			
			// we want to run up to 64 non-sync ops while holding the lease, or 8MB in between commits
			
			closeReceived := false // whether to shut down
			var syncChan chan bool = nil // 
			CONTINUATION_WRITE: for numWrites := 0 ; numWrites < numExtra ; numWrites++ {
				select {
					case myOp,ok := <- w.workQueue:
						if !ok {
							closeReceived = true
							break CONTINUATION_WRITE
						}
						// check if this op also modifies inode, if so,
						if myOp.data != nil || myOp.doTrunc {
							err := doWriteOrTrunc(op)
							if err != nil {
								fmt.Printf("Error writing in continuation loop %s\n",err)
								break CONTINUATION_WRITE
							}
						} 
						// if it's a sync, we break out of the loop and sync up
						if op.doSync {
							syncChan = op.doneChan
							break CONTINUATION_WRITE
						}
						if op.doClose {
							// op close
							syncChan = op.doneChan
							closeReceived = true
							break CONTINUATION_WRITE
						}
				    default: 
				    	// no ops in pipe, bail and release lease so others can write
				    	break CONTINUATION_WRITE
				}
			}
			w.inode = nil
			err = w.names.SetInode(w.inode)
			if err != nil {
					fmt.Printf("process() couldn't set inode %d after continuation write, quitting, err: %s..\n", w.inodeid,err)
					l.Release()
					return
			}
			err = l.Release()
			if err != nil {
					fmt.Printf("process() couldn't release lease for inode %d after continuation write, quitting, err: %s..\n", w.inodeid,err)
					return
			}
			if closeReceived {
				return
			}
			// end continuation write section
}

// assumes correct leases are held and does op using w.inode to store state
// may commit changes to master if we added a block, however most of the time will not
func (w *InodeWriter) doWriteOrTrunc(op *writeOp) error {
	if op.doTrunc {
		// commit
		w.names.SetInode(w.inode)
		// truncate
		w.names.Truncate(i.inodeid,op.truncLength)
		
	}
	if op.data != nil {
	
	}
	return nil
}


// calls out to name service to truncate this file by repeatedly shrinking blocks
func (w *InodeWriter) Truncate(length uint64) error {
	// pick up lease
	lease, err := w.leases.WritBROKENeLease(w.inodeid)
	
	// TODO wire this to workQueue
	
	defer lease.Release()
	if err != nil {
		return err
	}
	return w.names.Truncate(w.inodeid, length)
}

//io.InodeWriter
func (w *InodeWriter) WriteAt(p []byte, off uint64, length uint32) (nWritten uint32, err error) {
	// get our copy of buffer
	ourBuff := maggiefs.GetBuff()
	copy(ourBuff, p[:int(length)])
	op := &writeOp{ourBuff, 0, 0, false, 0, false, nil}
	w.workQueue <- op
	return length, nil
}

func (w *InodeWriter) Fsync() (err error) {
	fsync := &writeOp{nil, 0, 0, false, 0, true, make(chan bool)}
	w.workQueue <- fsync
	// don't return until done
	<-fsync.doneChan
	return nil
}

func (w *InodeWriter) Close() (err error) {
	fsync := &writeOp{nil, 0, 0, false, 0, true, make(chan bool)}
	w.workQueue <- fsync
	// don't return until done
	<-fsync.doneChan
	return nil
}

// assumes lease is held and passed in inode is up to date -- may flush inode state to nameserver if we're adding a new block
// but will not most of the time, will modify passed in inode instead
func (w *InodeWriter) doWrite(ino *maggiefs.Inode, p []byte, off uint64, length uint32) (uint32, error) {
	// check if we need to add blocks, note we support sparse files
	// so it's ok to add blocks that won't be written to right away,
	// on read they'll return 0 for all bytes in empty sections, and
	// won't read past
	if off+uint64(length) > w.inode.Length {
		err := w.addBlocksForFileWrite(off, length)
		if err != nil {
			return 0, err
		}
		//fmt.Printf("Added blocks for filewrite to ino %+v\n", inode)
	}
	// now write bytes
	nWritten := 0
	endOfWritePos := off + uint64(length) - 1
	startOfWritePos := off + uint64(nWritten)
	for _, b := range w.inode.Blocks {
		//fmt.Printf("evaluating block %+v for writeStartPos %d endofWritePos %d\n", b, startOfWritePos, endOfWritePos)
		// TODO do we need that last endOfWritePos <= b.EndPos here?
		if (b.StartPos <= startOfWritePos && b.EndPos > startOfWritePos) || (b.StartPos < endOfWritePos && endOfWritePos <= b.EndPos) {

			posInBlock := uint64(0)
			if b.StartPos < off {
				posInBlock += off - b.StartPos
			}
			//fmt.Printf("nWritten %d off %d len %d endofWritePos %d block %+v posInBlock %d\n", nWritten, off, length, endOfWritePos, b, posInBlock)
			writeLength := int(length) - nWritten
			if b.EndPos < endOfWritePos {
				writeLength = int(b.Length() - posInBlock)
			}
			//fmt.Printf("startIdx %d writeLength %d\n", nWritten, writeLength)
			startIdx := nWritten
			endIdx := startIdx + writeLength
			//fmt.Printf("Writing %d bytes to block %+v pos %d startIdx %d endIdx %d\n", b, posInBlock, startIdx, endIdx)
			err := w.datas.Write(b, p[startIdx:endIdx], posInBlock)
			if err != nil {
				return 0, err
			}
			//			fmt.Printf("Wrote %d bytes to block %+v\n", endIdx-startIdx, b)
			nWritten += writeLength
			//			fmt.Printf("Wrote %d, nWritten total %d", writeLength, nWritten)
		}
	}
	//fmt.Printf("Returning from write, nWritten %d",nWritten)
	return uint32(nWritten), nil
}

// acquires lease, then adds the blocks to the namenode,
// patching up the referenced inode to match
//
// only sets inode with nameservice IFF we had to add a block.
// in that case, we first set the inode so that we're consistent, then ask the
// namenode to add a new block of the required length
func (w *InodeWriter) addBlocksForFileWrite(off uint64, length uint32) error {
	//fmt.Printf("Adding/extending blocks for write at off %d length %d\n", off, length)
	newEndPos := off + uint64(length)
	if newEndPos > w.inode.Length {
		// if we have a last block and it's less than max length,
		// extend last block to max block length first
		//fmt.Printf("Adding/extending blocks for file write to inode %+v\n", inode)
		if w.inode.Blocks != nil && len(w.inode.Blocks) > 0 {
			idx := int(len(w.inode.Blocks) - 1)
			lastBlock := w.inode.Blocks[idx]
			if lastBlock.Length() < BLOCKLENGTH {
				extendLength := BLOCKLENGTH - lastBlock.Length()
				if lastBlock.EndPos+extendLength > off+uint64(length) {
					// only extend as much as we need to
					extendLength = off + uint64(length-1) - lastBlock.EndPos
				}
				lastBlock.EndPos = lastBlock.EndPos + extendLength
				w.inode.Blocks[idx] = lastBlock
				w.inode.Length += extendLength
				//fmt.Printf("Extended block %v on inode %v\n",lastBlock,inode)
			}
		}
		// and add new blocks as necessary
		for newEndPos > w.inode.Length {
			//fmt.Printf("New end pos %d still greater than inode length %d\n", newEndPos, inode.Length)
			// make suer
			newBlockLength := newEndPos - w.inode.Length
			if newBlockLength > BLOCKLENGTH {
				newBlockLength = BLOCKLENGTH
			}
			// write prev inode block state so we stay consistent with master while adding a block
			w.inode.Mtime = time.Now().Unix()
			err := w.names.SetInode(w.inode)
			if err != nil {
				return err
			}
			newBlock, err := w.names.AddBlock(w.inode.Inodeid, uint32(newBlockLength), w.localDnId)
			if err != nil {
				return err
			}
			// patch up local copy
			w.inode.Blocks = append(w.inode.Blocks, newBlock)
			w.inode.Length += newBlockLength
		}
	}
	return nil
}
