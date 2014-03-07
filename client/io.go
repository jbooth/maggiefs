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
		p.WriteHeader(0, 0)
		return nil
	}
	if position > inode.Length {
		return errors.New("Read past end of file")
	}
	if position+uint64(length) > inode.Length {
		// truncate length to the EOF
		length = uint32(inode.Length - position)
	}
	// confirm currBlock and currReader correct
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
		err = datas.Read(block, p, posInBlock, numBytesFromBlock)
		if err != nil && err != io.EOF {
			return fmt.Errorf("reader.go error reading from block %+v : %s", block, err.Error())
		}
		nRead += numBytesFromBlock
		position += uint64(numBytesFromBlock)
		//fmt.Printf("finished reading a block, nRead %d, pos %d, total to read %d\n",nRead,position,length)
	}
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
	leases        maggiefs.LeaseService
	names         maggiefs.NameService
	datas         maggiefs.DataService
	myDnId        *uint32
	inodeid       uint64
	pendingWrites chan pendingWrite
	l             *sync.Mutex
	closed        bool
}

func NewWriter(inodeid uint64, leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService, myDnId *uint32) *Writer {
	ret := &Writer{leases, names, datas, myDnId, inodeid, make(chan pendingWrite, 64), new(sync.Mutex), false}
	go ret.process()
	return ret
}

// represents either an outstanding write or a sync request
type pendingWrite struct {
	done   chan uint64
	isSync bool
}

// this method greedily pulls as many inode updates as it can in between actually updating
func (w *Writer) process() {
	for {
		// this will be non-nil if we have a sync request to inform when up to date
		syncRequest := pendingWrite{nil, true}
		// pull one update in blocking mode
		write, ok := <-w.pendingWrites
		if !ok {
			return
		}
		updates := []pendingWrite{write}
		// pull as many updates as we can without blocking
		numUpdates := 0
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
				updates = append(updates, write)
				numUpdates += 1
				if numUpdates > 128 {
					break INNER
				}
			default:
				break INNER
			}
		}
		// coalesce
		newLen := uint64(0)
		for _, w := range updates {
			l := <-w.done
			if l > newLen {
				newLen = l
			}
		}
		if newLen > 0 {
			_, err := w.names.Extend(w.inodeid, newLen)
			if err != nil {
				log.Printf("Error extending ino %d : %s", err)
			}
			err = w.leases.Notify(w.inodeid)
			fmt.Printf("Finished updating ino %d to len %d\n", w.inodeid, newLen)
			newLen = 0
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
		fmt.Printf("Adding a block..")
		blockPos := uint64(0)
		if len(inode.Blocks) > 0 {
			blockPos = inode.Blocks[len(inode.Blocks)-1].EndPos + 1
		}
		inode, err = w.names.AddBlock(inode.Inodeid, blockPos, w.myDnId)
	}
	writes := blockwrites(inode, p, position, length)
	for _, wri := range writes {
		pending := pendingWrite{make(chan uint64, 1), false}
		w.pendingWrites <- pending
		lengthAtEndOfWrite := wri.b.StartPos + wri.posInBlock + uint64(len(wri.p)) + 1
		err = w.datas.Write(wri.b, wri.p, wri.posInBlock, func() {
			//fmt.Printf("Finished write, ino length %d, in callback now \n", lengthAtEndOfWrite)
			pending.done <- lengthAtEndOfWrite
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) Sync() error {
	syncRequest := pendingWrite{make(chan uint64), true}
	w.l.Lock()
	if w.closed {
		w.l.Unlock()
		return fmt.Errorf("Can't sync, already closed!")
	}
	w.pendingWrites <- syncRequest
	w.l.Unlock()
	// since syncRequest is unbuffered, this will block until processed
	syncRequest.done <- 0
	return nil
}

func (w *Writer) Close() error {
	w.l.Lock()
	defer w.l.Unlock()
	w.closed = true
	syncRequest := pendingWrite{make(chan uint64), true}
	w.pendingWrites <- syncRequest
	syncRequest.done <- 0
	close(w.pendingWrites)
	return nil
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
