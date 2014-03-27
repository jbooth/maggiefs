package client

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/fuse"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/splice"
	"io"
	"log"
	"sync"
	"sync/atomic"
)

// stateless function to serve random reads
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

func NewReader() *Reader {
	return &Reader{new(sync.Mutex), 0, nil, 0, 0, 0}
}

type Reader struct {
	l                      *sync.Mutex  // guards readahead state
	lastReadEndPos         uint64       // heuristic for whether to switch to readahead mode -- 2 sequential reads means readahead
	aheadPipe              *splice.Pair // pipe for readahead
	aheadPos               uint64       // inode position of first byte in readahead pipe
	aheadPending           uint32       // how many bytes are pending to the pipe -- might be in pipe or might not be yet
	aheadRequestUnfinished uint32       // this is set to >0 if we have a request running that's not finished yet
}

func (r *Reader) Read(datas maggiefs.DataService, inode *maggiefs.Inode, p fuse.ReadPipe, position uint64, length uint32) (err error) {
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
	blockForRead, err := blockForPos(position, inode)
	if err != nil {
		return err
	}

	// if this read doesn't line up with pipe, treat it as a random read
	// first read should also always be random
	if ((position + uint64(length)) > blockForRead.EndPos) || position == 0 {
		r.l.Lock()
		if r.lastReadEndPos == position {
			// start readahead
			err = r.initReadahead(position)
			if err != nil {
				return err
			}
			err = r.fillMore(datas, inode)
			if err != nil {
				return err
			}
		} else {
			// serve random read without lock
			r.lastReadEndPos = position + uint64(length)
			r.l.Unlock()
			return Read(datas, inode, p, position, length)
		}
	}
	// serving from readahead pipe
	defer r.l.Unlock()
	// write header
	err = p.WriteHeader(0, int(length))
	if err != nil {
		log.Printf("Error writing resp header to splice pipe : %s", err)
		return err
	}
	nRead := uint32(0)
	for nRead < length {
		// serve what we can from pipe
		numToSplice := length
		if r.aheadPending < length {
			numToSplice = r.aheadPending
		}
		if numToSplice > 0 {
			log.Printf("Splicing %d from readahead", numToSplice)
			numSpliced, err := p.LoadFrom(r.aheadPipe.ReadFd(), int(numToSplice))
			if err != nil {
				return err
			}
			if numSpliced < 0 {
				return fmt.Errorf("Splice returned -1!")
			}
			r.aheadPos += uint64(numSpliced)
			r.aheadPending -= uint32(numSpliced)
			nRead += uint32(numSpliced)
		}
		// issue request to send more to pipe
		err = r.fillMore(datas, inode)
		if err != nil {
			return err
		}
	}
	return p.Commit()
}

func (r *Reader) Close() error {
	err := r.drain()
	splice.Done(r.aheadPipe)
	return err
}

var pipeSize = 512 * 1024

func (r *Reader) initReadahead(startPos uint64) (err error) {
	r.aheadPos = startPos
	r.aheadPending = 0
	if r.aheadPipe == nil {
		r.aheadPipe, err = splice.Get()
		if err != nil {
			return err
		}
		err = r.aheadPipe.Grow(pipeSize)
		if err != nil {
			return err
		}
	} else {
		return r.drain()
	}
	return nil
}

var drain = make([]byte, 128*1024)

func (r *Reader) drain() (err error) {
	drainTo := drain[:]
	if r.aheadPending < uint32(len(drainTo)) {
		drainTo = drainTo[:int(r.aheadPending)]
	}
	for r.aheadPending > 0 {
		n, err := r.aheadPipe.Write(drainTo)
		r.aheadPending -= uint32(n)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Reader) fillMore(datas maggiefs.DataService, i *maggiefs.Inode) (err error) {
	// if there's already an unfinished request running, don't do anything
	alreadyReading := atomic.LoadUint32(&r.aheadRequestUnfinished)
	if alreadyReading > 0 {
		return nil
	}
	// same if we have enough pending bytes to fill the pipe already
	if r.aheadPending == uint32(pipeSize) {
		return nil
	}
	// send a read request to fill the pipe
	posToRead := r.aheadPos + uint64(r.aheadPending)
	block, err := blockForPos(posToRead, i)
	if err != nil {
		return err
	}
	posInBlock := posToRead - block.StartPos
	numBytesFromBlock := uint32(block.Length()) - uint32(posInBlock)
	// only request as many as our pipe can handle
	pipeCapacity := uint32(pipeSize) - r.aheadPending
	if numBytesFromBlock > pipeCapacity {
		numBytesFromBlock = pipeCapacity
	}
	// mark a read in progress, update our state
	atomic.StoreUint32(&r.aheadRequestUnfinished, 1)
	r.aheadPending += numBytesFromBlock
	// fire a read which marks done
	err = datas.Read(block, r.aheadPipe, posInBlock, numBytesFromBlock, func() {
		atomic.StoreUint32(&r.aheadRequestUnfinished, 0)
	})
	return err
}
