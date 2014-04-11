package client

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"log"
	"sync"
)

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

type writeResult struct {
	err    error
	off    int64
	length int64
}

// represents either an outstanding write or a sync request
type pendingWrite struct {
	done   chan writeResult
	isSync bool
}

// this method greedily pulls as many inode updates as it can in between actually updating
func (w *Writer) process() {
	closed := false
	for {
		if closed {
			return
		}
		// pull one update in blocking mode
		write, ok := <-w.pendingWrites
		if !ok {
			return
		}
		if write.isSync {
			//log.Printf("Finishing sync request for ino %d : %+v", w.inodeid, write)
			<-write.done
			continue
		}
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
					closed = true
					break INNER
				}
				if write.isSync {
					syncRequest = write
					break INNER
				}
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
			newOffAndLen := <-write.done
			if newOffAndLen.err != nil {
				log.Printf("Error on write!  Implement proper behavior here, seriously!")
			}
			if newOff == 0 && newLen == 0 {
				// don't have a previous write we're extending, so set them and reloop
				newOff = newOffAndLen.off
				newLen = newOffAndLen.length
			} else if newOff+newLen == newOffAndLen.off {
				// aligns with our existing write, so extend before notifying
				newLen += newOffAndLen.length
			} else {
				// we have a previous write that needs flushing

				//log.Printf("Updating master with off %d len %d", newOff, newLen)
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
			//log.Printf("Updating master with off %d len %d", newOff, newLen)
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
			//log.Printf("Finishing sync request for ino %d : %+v", w.inodeid, syncRequest)
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
		//log.Printf("Adding a block to inode %+v..", inode)
		blockPos := uint64(0)
		if len(inode.Blocks) > 0 {
			blockPos = inode.Blocks[len(inode.Blocks)-1].EndPos + 1
		}
		inode, err = w.names.AddBlock(inode.Inodeid, blockPos, w.myDnId)
		if err != nil {
			return err
		}
		//log.Printf("Added a block, new inode %+v", inode)
		// invalidate local ino cache to make sure future reads see this block
		// 0 length and offset prevent spurious cluster notify
		w.doNotify(inode.Inodeid, 0, 0)

	}
	writes := blockwrites(inode, p, position, length)
	for _, wri := range writes {
		pending := pendingWrite{make(chan writeResult, 1), false}
		w.pendingWrites <- pending
		err = w.datas.Write(wri.b, wri.p, wri.posInBlock, func(err error) {
			finishedWrite := writeResult{err, int64(wri.b.StartPos + wri.posInBlock), int64(len(wri.p))}
			//log.Printf("Finished write, operation %+v, in callback now \n", finishedWrite)
			pending.done <- finishedWrite
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Sync() error {
	w.l.Lock()
	defer w.l.Unlock()
	w.doSync()
	return nil
}

func (w *Writer) Close() error {
	w.l.Lock()
	defer w.l.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	w.doSync()
	close(w.pendingWrites)
	return nil
}

func (w *Writer) doSync() {
	if w.closed {
		return
	}
	syncRequest := pendingWrite{make(chan writeResult), true}
	w.pendingWrites <- syncRequest
	// since syncRequest is unbuffered, this will block until processed
	syncRequest.done <- writeResult{nil, 0, 0}
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
