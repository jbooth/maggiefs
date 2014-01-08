package client

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/fuse"
	"github.com/jbooth/maggiefs/maggiefs"
	"io"
)

func NewReader(inodeid uint64, names maggiefs.NameService, datas maggiefs.DataService) (r *Reader, err error) {
	return &Reader{inodeid, names, datas}, nil
}

// represents an open file
// maintains a one page buffer for reading
// for writable or RW files, see OpenWriteFile
type Reader struct {
	inodeid uint64
	names   maggiefs.NameService
	datas   maggiefs.DataService
}

func (r *Reader) ReadAt(p fuse.ReadPipe, position uint64, length uint32) (err error) {
	// have to re-get inode every time because it might have changed

	inode, err := r.names.GetInode(r.inodeid)
	if err != nil {
		return err
	}
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
		//fmt.Printf("reader.go reading from block %+v at posInBlock %d, length %d array offset %d \n",block,posInBlock,numBytesFromBlock,offset)
		err = r.datas.Read(block, p, posInBlock, numBytesFromBlock)
		if err != nil && err != io.EOF {
			return fmt.Errorf("reader.go error reading from block %+v : %s", block, err.Error())
		}
		nRead += numBytesFromBlock
		position += uint64(numBytesFromBlock)
		//fmt.Printf("reader.go finished reading a block, nRead %d, pos %d, total to read %d\n",nRead,position,length)
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

//io.Closer
func (r *Reader) Close() error {
	return nil
}
