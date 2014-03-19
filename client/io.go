package client

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/fuse"
	"github.com/jbooth/maggiefs/maggiefs"
	"io"
	"log"
	"sync"
)

func Read(datas maggiefs.DataService, inode *maggiefs.Inode, p fuse.ReadPipe, position uint64, length uint32) (err error) {
	if position == inode.Length {
		// write header for OK, 0 bytes at EOF
		log.Printf("Read at EOF, position %d length %d, returning 0", position, inode.Length)
		p.WriteHeader(0, 0)
		return nil
	}
	if position > inode.Length {
		return errors.New("Read past end of file")
	}
	if position+uint64(length) > inode.Length {
		// truncate length to the EOF
		log.Printf("Truncating length from %d to %d", length, inode.Length-position)
		length = uint32(inode.Length - position)
	}
	// write header
	err = p.WriteHeader(0, int(length))
	if err != nil {
		log.Printf("Error writing resp header to splice pipe : %s", err)
		return err
	}

	// splice bytes and commit
	nRead := uint32(0)
	for nRead < length {
		if position == inode.Length {
			break
		}
		block, err := blockForPos(position, inode)
		if err != nil {
			return err
		}
		// read at most the bytes remaining in this block
		// if we're being asked to read past end of block, we just return early
		posInBlock := uint64(position) - block.StartPos
		numBytesFromBlock := uint32(block.Length()) - uint32(posInBlock)
		if numBytesFromBlock > length-nRead {
			numBytesFromBlock = length - nRead
		}
		if posInBlock == block.Length() {
			// bail out and fill in with 0s
			break
		}
		// read bytes
		//fmt.Printf("reading from block %+v at posInBlock %d, length %d array offset %d \n",block,posInBlock,numBytesFromBlock,offset)
		if numBytesFromBlock == length-nRead {
			// if the rest of the read is coming from this block, read and commit
			// note this call is async, not done when we return
			onDone := make(chan bool, 1)
			err = datas.Read(block, p, posInBlock, numBytesFromBlock, func() {
				e1 := p.Commit()
				if e1 != nil {
					log.Printf("Err committing to pipe from remote read of block %+v, read at posInBlock %d", block, posInBlock)
				}
				onDone <- true
			})
			<-onDone
		} else {
			// else, read and wait till done, commit next time around
			onDone := make(chan bool, 1)
			err = datas.Read(block, p, posInBlock, numBytesFromBlock, func() { onDone <- true })
			<-onDone
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("reader.go error reading from block %+v : %s", block, err.Error())
		}
		nRead += numBytesFromBlock
		position += uint64(numBytesFromBlock)
		//fmt.Printf("finished reading a block, nRead %d, pos %d, total to read %d\n",nRead,position,length)
	}
	log.Printf("Done with read, successfully read %d out of %d", nRead, length)
	// sometimes the length can be more bytes than there are in the file, so always just give that back
	return nil
}

func blockForPos(position uint64, inode *maggiefs.Inode) (blk maggiefs.Block, err error) {
	for i := 0; i < len(inode.Blocks); i++ {
		blk := inode.Blocks[i]
		//fmt.Printf("Checking block %+v to see if position %d fits\n",blk,position)
		if position >= blk.StartPos && position < blk.EndPos {
			return blk, nil
		}
	}
	return maggiefs.Block{}, errors.New(fmt.Sprintf("offset %d not found in any blocks for inode %d, bad file?", position, inode.Inodeid))

}

// manages async writes
type Writer struct {
	doNotify      func(inodeid uint64, offset int64, length int64) error
	names         maggiefs.NameService
	datas         maggiefs.DataService
	myDnId        *uint32
	inodeid       uint64
	pendingWrites chan pendingWrite
	l             *sync.Mutex
	closed        bool
}

// on construction, we pass in a wrapper for the notify method which invalidates openFileMap's inode cache
func NewWriter(inodeid uint64, doNotify func(inodeid uint64, offset int64, length int64) error, names maggiefs.NameService, datas maggiefs.DataService, myDnId *uint32) *Writer {
	ret := &Writer{doNotify, names, datas, myDnId, inodeid, make(chan pendingWrite, 16), new(sync.Mutex), false}
	go ret.process()
	return ret
}

type offAndLen struct {
	off    int64
	length int64
}

// represents either an outstanding write or a sync request
type pendingWrite struct {
	done   chan offAndLen
	isSync bool
}

// this method greedily pulls as many inode updates as it can in between actually updating
func (w *Writer) process() {
	for {
		// pull one update in blocking mode
		write, ok := <-w.pendingWrites
		if !ok {
			return
		}
		if write.isSync {
			<-write.done
			continue
		}
		log.Printf("process() Got write %+v from channel in blocking call", write)
		updates := []pendingWrite{write}
		numUpdates := 1
		// this will be non-nil if we have a sync request to inform when up to date
		syncRequest := pendingWrite{nil, true}
		// pull up to 32 updates as we can without blocking
	INNER:
		for {
			select {
			case write, ok = <-w.pendingWrites:
				if !ok {
					break INNER
				}
				if write.isSync {
					syncRequest = write
					break INNER
				}
				log.Printf("process() Got write %+v from channel w/ nonblocking call", write)
				updates = append(updates, write)
				numUpdates += 1
				if numUpdates > 32 {
					break INNER
				}
			default:
				break INNER
			}
		}
		// coalesce updates if we can
		newOff := int64(0)
		newLen := int64(0)
		for _, write := range updates {
			log.Printf("Handling update %+v", write)
			newOffAndLen := <-write.done
			log.Printf("New off and len : %+v", newOffAndLen)
			if newOff == 0 && newLen == 0 {
				// don't have a previous write we're extending, so set them and reloop
				newOff = newOffAndLen.off
				newLen = newOffAndLen.length
			} else if newOff+newLen == newOffAndLen.off {
				// aligns with our existing write, so extend before notifying
				newLen += newOffAndLen.length
			} else {
				// we have a previous write that needs flushing

				log.Printf("Updating master with off %d len %d", newOff, newLen)
				// make sure namenode thinks inode is at least this long
				_, err := w.names.Extend(w.inodeid, uint64(newLen+newOff))
				if err != nil {
					log.Printf("Error extending ino %d to length %d", w.inodeid, uint64(newLen+newOff))
				}
				// notify other nodes in cluster
				err = w.doNotify(w.inodeid, newOff, newLen)
				if err != nil {
					log.Printf("Error notifying on ino %d to length %d", w.inodeid, uint64(newLen+newOff))
				}
				// re-initialize vals
				newOff = newOffAndLen.off
				newLen = newOffAndLen.length
			}
		}
		if newLen > 0 {
			log.Printf("Updating master with off %d len %d", newOff, newLen)
			// one last update
			_, err := w.names.Extend(w.inodeid, uint64(newOff+newLen))
			if err != nil {
				log.Printf("Error extending ino %d to length %d", w.inodeid, uint64(newLen+newOff))
			}
			err = w.doNotify(w.inodeid, newOff, newLen)
			if err != nil {
				log.Printf("Error notifying on ino %d to length %d", w.inodeid, uint64(newLen+newOff))
			}
		}
		if syncRequest.done != nil {
			<-syncRequest.done
			syncRequest = pendingWrite{nil, true}
		}
	}
}

func (w *Writer) Write(datas maggiefs.DataService, inode *maggiefs.Inode, p []byte, position uint64, length uint32) (err error) {
	w.l.Lock()
	defer w.l.Unlock()
	if w.closed {
		return fmt.Errorf("Can't write, already closed!")
	}
	if len(inode.Blocks) == 0 || inode.Blocks[len(inode.Blocks)-1].EndPos < position+uint64(length) {
		//log.Printf("Syncing before adding block")
		//w.doSync()
		log.Printf("Adding a block to inode %+v..", inode)
		blockPos := uint64(0)
		if len(inode.Blocks) > 0 {
			blockPos = inode.Blocks[len(inode.Blocks)-1].EndPos + 1
		}
		inode, err = w.names.AddBlock(inode.Inodeid, blockPos, w.myDnId)
		if err != nil {
			return err
		}
		log.Printf("Added a block, new inode %+v", inode)
		// invalidate local ino cache to make sure future reads see this block
		// 0 length and offset prevent spurious cluster notify
		w.doNotify(inode.Inodeid, 0, 0)

	}
	writes := blockwrites(inode, p, position, length)
	for _, wri := range writes {
		pending := pendingWrite{make(chan offAndLen, 1), false}
		w.pendingWrites <- pending
		err = w.datas.Write(wri.b, wri.p, wri.posInBlock, func() {
			finishedWrite := offAndLen{int64(wri.b.StartPos + wri.posInBlock), int64(len(wri.p))}
			log.Printf("Finished write, operation %+v, in callback now \n", finishedWrite)
			pending.done <- finishedWrite
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Sync() error {
	// questionable optimization, we let go of the lock while waiting on sync,
	// so other writes can get in behind us and keep the pipeline full
	syncRequest := pendingWrite{make(chan offAndLen), true}
	w.l.Lock()
	if w.closed {
		w.l.Unlock()
		return fmt.Errorf("Can't sync, already closed!")
	}
	w.pendingWrites <- syncRequest
	w.l.Unlock()
	// since syncRequest is unbuffered, this will block until processed
	syncRequest.done <- offAndLen{0, 0}
	return nil
}

func (w *Writer) Close() error {
	w.l.Lock()
	defer w.l.Unlock()
	w.closed = true
	w.doSync()
	close(w.pendingWrites)
	return nil
}

func (w *Writer) doSync() {
	syncRequest := pendingWrite{make(chan offAndLen), true}
	w.pendingWrites <- syncRequest
	// since syncRequest is unbuffered, this will block until processed
	syncRequest.done <- offAndLen{0, 0}
}

type blockwrite struct {
	b          maggiefs.Block
	p          []byte
	posInBlock uint64
}

// gets the list of block writes
func blockwrites(i *maggiefs.Inode, p []byte, off uint64, length uint32) []blockwrite {
	nWritten := 0
	startOfWritePos := off
	endOfWritePos := off + uint64(length) - 1
	ret := make([]blockwrite, 0)
	for _, b := range i.Blocks {
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
			startIdx := nWritten
			endIdx := startIdx + writeLength
			if endIdx-startIdx > 0 {
				ret = append(ret, blockwrite{b, p[startIdx:endIdx], posInBlock})
			}
			nWritten += writeLength
			//fmt.Printf("Writing %d bytes to block %+v pos %d startIdx %d endIdx %d\n", b, posInBlock, startIdx, endIdx)
			//      fmt.Printf("Wrote %d bytes to block %+v\n", endIdx-startIdx, b)
			//      fmt.Printf("Wrote %d, nWritten total %d", writeLength, nWritten)
		}
	}
	return ret
}
