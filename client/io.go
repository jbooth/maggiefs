package client

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/fuse"
	"github.com/jbooth/maggiefs/maggiefs"
	"io"
)

func doRead(datas maggiefs.DataService, inode *maggiefs.Inode, p fuse.ReadPipe, position uint64, length uint32) (err error) {
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
	names         maggiefs.NameService
	inodeid       uint64
	pendingWrites chan pendingWrite
	l             *sync.Mutex
	closed        bool
}

// represents either an outstanding write or a sync request
type pendingWrite struct {
	done   chan uint64
	isSync bool
}

// this convoluted function processes finished writes, updating the namenode with a new inode length if necessary
func (w *Writer) process() {
	var newLen uint64 = 0
	var currPending pendingWrite = pendingWrite{nil, false}
	var hasMore bool = true
	for {
		if !hasMore {
			return
		}
		// make a single blocking call so we don't busy-loop
		if currPending.done != nil {
			currPending, hasMore = <-w.pendingWrites
			if !hasMore {
				return
			}
		}
		l := <-currPending.done
		if l > newLen {
			newLen = l
		}
		currPending = pendingWrite{nil, false}
		// now pull as many as we can until we reach either a sync, or an incomplete write which would have blocked
	INNER:
		for {
			select {
			// pull until we get a sync or we would block
			case currPending, hasMore = <-w.pendingWrites:
				if !hasMore || currPending.isSync {
					// channel closed or sync request, break out
					break INNER
					// state:  currPending is a sync request or we're about to terminate
					// will either finish sync on reloop or terminate
				} else {
					select {
					// if this pending write is finished, reloop and pull another
					case l := <-currPending.done:
						if l > newLen {
							newLen = l
						}
						currPending = pendingWrite{nil, false}
						// state:  currPending invalid, reloop INNER and pull another if we can
					default:
						break INNER
						// state:  currPending is valid and unfinished, will block on it on re-loop
					}
				}
			default:
				break INNER
			}
		}
		// update node length if appropriate before relooping
		if newLen > 0 {
			_, err := w.names.Extend(w.inodeid, newLen)
			newLen = 0
		}
	}
}
func (w *Writer) doWrite(datas maggiefs.DataService, inode *maggiefs.Inode, p []byte, position uint64, length uint32) (err error) {

}

func (w *Writer) sync() {

}

func (w *Writer) close() {

}
