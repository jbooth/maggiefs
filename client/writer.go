package client

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
)

type Writer struct {
	inodeid uint64
	leases  maggiefs.LeaseService
	names   maggiefs.NameService
	datas   maggiefs.DataService
}

func NewWriter(inodeid uint64, leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService) (w *Writer, err error) {
	if err != nil {
		return nil, err
	}

	return &Writer{inodeid, leases, names, datas}, nil
}

func (w *Writer) Truncate(length uint64) error {
	// calls out to name service to truncate this file by repeatedly shrinking blocks
	return nil
}

//io.Writer
func (w *Writer) WriteAt(p []byte, off uint64, length uint32) (written uint32, err error) {
	// pick up lease
	lease, err := w.leases.WriteLease(w.inodeid)
	defer lease.Release()
	if err != nil {
		return 0, err
	}
	inode, err := w.names.GetInode(w.inodeid)
	if err != nil {
		return 0, err
	}

	// first figure which block we're supposed to be at
	// if offset is greater than length, we can't write (must append from eof)
	if off > inode.Length {
		return 0, errors.New("offset > length of file")
	}
	currBlock, err := blockForPos(off, inode)
	if err != nil {
		return 0, err
	}
	if off == inode.Length {	 
		// we are appending,
		// need to either extend current block or add a new one if we're just starting a file or at the end of current block
		currLen := currBlock.EndPos - currBlock.StartPos
		if off == 0 || currLen == BLOCKLENGTH {
			fmt.Printf("adding block\n")
			// add a new block
			blockLength := length
			if blockLength > uint32(BLOCKLENGTH) {
				blockLength = uint32(BLOCKLENGTH)
			}
			currBlock, err = w.names.AddBlock(inode.Inodeid, blockLength)
			if err != nil {
				return 0, err
			}
		} else if currLen == inode.Length {
			fmt.Printf("adding to end of current block\n")
  		// extend currBlock to min(currLen + len, BLOCKLENGTH)
			maxAddedByte := BLOCKLENGTH - (currLen)
			if uint64(length) > maxAddedByte {
				length = uint32(maxAddedByte)
			}
			currBlock, err = w.names.ExtendBlock(inode.Inodeid, currBlock.Id, length)
			if err != nil {
				return 0, err
			}
		}

	} else {
		// find existing block we are random writing into
		currBlock, err := blockForPos(off, inode)
		if err != nil {
			return 0, err
		}
		fmt.Printf("block for pos %+v\n", currBlock)
		if currBlock.EndPos-currBlock.StartPos < uint64(length) {
			// block isn't long enough, need to extend and/or curtail write
			// extend currBlock to min(currLen + len, BLOCKLENGTH)
			currLen := currBlock.EndPos - currBlock.StartPos
			maxAddedByte := BLOCKLENGTH - (currLen)
			if uint64(length) > maxAddedByte {
				length = uint32(maxAddedByte)
			}
			currBlock, err = w.names.ExtendBlock(inode.Inodeid, currBlock.Id, length)
			if err != nil {
				return 0, err
			}
		}
	}
	// now write bytes
	err = w.datas.Write(currBlock, p[0:length], off)
	// TODO should update Mtime
	//w.currBlock.Mtime = int64(time.Now().Unix())
	return length, err
}

func (w *Writer) Fsync() (err error) {
	l,err := w.leases.WriteLease(w.inodeid)
	if err != nil { return err }
	l.Release()
	return nil
}

func (f *Writer) Close() (err error) {
	return nil
}
