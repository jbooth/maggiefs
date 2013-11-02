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
	queueIn  chan *writeOp
	queueOut chan *writeOp // 2 queues with goroutine in between for unbounded, ordered queue
	l        maggiefs.WriteLease
	inode    *maggiefs.Inode
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

func (w *writeOp) wait() {
	<-w.doneChan
}

func NewInodeWriter(inodeid uint64, leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService, localDnId *uint32) (w *InodeWriter, err error) {
	if err != nil {
		return nil, err
	}

	ret := &InodeWriter{inodeid, leases, names, datas, localDnId, make(chan *writeOp),make(chan *writeOp),nil,nil}
	go ret.bufferChans()
	go ret.process()
	return ret,nil
}

func (w *InodeWriter) bufferChans() {
	defer close(w.queueOut)

	// pending events (this is the "infinite" part)
	pending := []*writeOp{}
	pendingAdded := 0
recv:
	for {
		// Ensure that pending always has values so the select can
		// multiplex between the receiver and sender properly
		if len(pending) == 0 {
			// realloc so we don't grow forever
			if pendingAdded > 1024 {
				pendingAdded = 0
				pending = []*writeOp{}
			}
			// no work to do right now, send sync to let go of lease so others can process
			w.queueOut <- &writeOp{nil, 0, 0, false, 0, true, nil}
			// now wait for input
			v, ok := <-w.queueIn
			if !ok {
				// in is closed, flush values
				break
			}

			// We now have something to send
			pending = append(pending, v)
			pendingAdded++
		}

		select {
		// Queue incoming values
		case v, ok := <-w.queueIn:
			if !ok {
				// in is closed, flush values
				break recv
			}
			pending = append(pending, v)
			pendingAdded++
		// Send queued values
		case w.queueOut <- pending[0]:
			pending = pending[1:]
		}
	}

	// After in is closed, we may still have events to send
	for _, v := range pending {
		w.queueOut <- v
	}
}

func (w *InodeWriter) process() {
	for op := range w.queueOut {
		var err error
		if op.data != nil || op.doTrunc {
			// need to ensure lease for these ops
			err = w.ensureLease()
			if err != nil {
				fmt.Printf("Goroutine couldn't acquire writeLease for inode %d, quitting..\n", w.inodeid)
				return
			}
			if op.data != nil {
				_, err = w.doWrite(op.data, op.startPos, op.length)
			} else if op.doTrunc {
				err = w.names.Truncate(w.inodeid,op.truncLength)
			}
		}
		if op.doSync {
			// only bother with sync if we actually hold lease
			if w.l != nil {
				err = w.dropLease()
				if err != nil {
					fmt.Printf("Goroutine couldn't commit changes for inode %+v, quitting..\n", w.inode)
				}
			}
		}
		if err != nil {
			fmt.Printf("Error with op %+v, continuing..",op)
		}
		if op.doneChan != nil {
			// inform blocking ops that they're done
			op.doneChan <- true
		}
	}
}
func (w *InodeWriter) ensureLease() error {
	var err error
	if w.l == nil {
		w.l, err = w.leases.WriteLease(w.inodeid)
		if err != nil {
			return err
		}
		w.inode, err = w.names.GetInode(w.inodeid)
	}
	return err
}

func (w *InodeWriter) dropLease() error {
	defer w.l.Release()
	return w.names.SetInode(w.inode)
}

// calls out to name service to truncate this file by repeatedly shrinking blocks
func (w *InodeWriter) Truncate(length uint64) error {
	// pick up lease
	lease, err := w.leases.WriteLease(w.inodeid)
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
	copy(ourBuff,p[:int(length)])
	op := &writeOp{ourBuff, 0, 0, false, 0, false, nil}
	w.queueIn <- op
	return length, nil
}

func (w *InodeWriter) Fsync() (err error) {
	fsync := &writeOp{nil, 0, 0, false, 0, true, make(chan bool)}
	w.queueIn <- fsync
	<- fsync.doneChan
	return nil
}

func (w *InodeWriter) Close() (err error) {	
	fsync := &writeOp{nil, 0, 0, false, 0, true, make(chan bool)}
	w.queueIn <- fsync
	<- fsync.doneChan
	return nil
}

// assumes lease is held and w.inode is up to date -- may flush w.inode state to nameserver if we're adding a new block
// but will not most of the time
func (w *InodeWriter) doWrite(p []byte, off uint64, length uint32) (uint32, error) {
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
