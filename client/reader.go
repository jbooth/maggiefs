package client

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/fuse"
	"github.com/jbooth/maggiefs/maggiefs"
	"log"
)

// maggiefs.Iocb
type readIocb struct {
	p      fuse.ReadPipe
	onDone chan error
}

func (r readIocb) OnSuccess() error {
	r.onDone <- r.p.Commit()
	return nil
}

func (r readIocb) OnErr(err error) {
	r.onDone <- err
}

// stateless function to serve random reads
func Read(datas maggiefs.DataService, inode *maggiefs.Inode, p fuse.ReadPipe, position uint64, length uint32) (err error) {
	if position == inode.Length {
		// write header for OK, 0 bytes at EOF
		log.Printf("Read at EOF, position %d length %d, returning 0", position, inode.Length)
		p.WriteHeader(0, 0)
		p.Commit()
		return nil
	}
	if position > inode.Length {
		return errors.New("Read past end of file")
	}
	if position+uint64(length) > inode.Length {
		// truncate length to the EOF
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

		onDone := make(chan error, 1)
		// send read request
		if numBytesFromBlock == length-nRead {
			// if the rest of the read is coming from this block, read and commit
			// note this call is async, not done when we return
			err = datas.Read(block, p, posInBlock, numBytesFromBlock, func(err error) {
				// bail on error
				if err != nil {
					onDone <- err
					return
				}
				// commit to fuse server on success
				e1 := p.Commit()
				if e1 != nil {
					onDone <- fmt.Errorf("Err (%s) committing to pipe from remote read of block %+v, read at posInBlock %d", e1, block, posInBlock)
				} else {
					onDone <- nil
				}
			})
		} else {
			// else, read and wait till done, commit next time around
			err = datas.Read(block, p, posInBlock, numBytesFromBlock, func(err error) { onDone <- err })
		}
		// check no err sending read request
		if err != nil {
			return fmt.Errorf("reader.go error reading from block %+v : %s", block, err.Error())
		}
		// wait done
		err = <-onDone
		if err != nil {
			return fmt.Errorf("reader.go error reading from block %+v : %s", block, err.Error())
		}
		nRead += numBytesFromBlock
		position += uint64(numBytesFromBlock)
		//fmt.Printf("finished reading a block, nRead %d, pos %d, total to read %d\n",nRead,position,length)
	}
	//log.Printf("Done with read, successfully read %d out of %d", nRead, length)
	// sometimes the length can be more bytes than there are in the file, so always just give that back
	return nil
}
