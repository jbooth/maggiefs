package client

import (
	"errors"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"sync"
	"time"
)

type Writer struct {
	inode      *maggiefs.Inode
	currBlock  maggiefs.Block
	currWriter maggiefs.BlockWriter
	names      maggiefs.NameService
	datas      maggiefs.DataService
	l          *sync.Mutex
}

func NewWriter(inodeid uint64, names maggiefs.NameService, datas maggiefs.DataService) (w *Writer, err error) {
	inode, err := names.GetInode(inodeid)
	if err != nil {
		return nil, err
	}

	return &Writer{inode, maggiefs.Block{}, nil, names, datas, new(sync.Mutex)}, nil
}

//io.Writer
func (w *Writer) WriteAt(p []byte, off uint64, length uint32) (written uint32, err error) {
	w.l.Lock()
	defer w.l.Unlock()
	// first figure which block we're supposed to be at
	// if offset is greater than length, we can't write (must append from eof)
	if off > w.inode.Length {
		return 0, errors.New("offset > length of file")
	}
	if off == w.inode.Length {
		currLen := w.currBlock.EndPos - w.currBlock.StartPos
		// we need to either extend current block or add a new one if we're just starting a file or at the end of current block
		if off == 0 || currLen == BLOCKLENGTH {
			fmt.Printf("adding block\n")
			// add a new block
			blockLength := length
			if blockLength > uint32(BLOCKLENGTH) {
				blockLength = uint32(BLOCKLENGTH)
			}
			w.currBlock, err = w.names.AddBlock(w.inode.Inodeid, blockLength)
			if err != nil {
				return 0, err
			}
			w.inode.Blocks = append(w.inode.Blocks, w.currBlock)
			w.currWriter, err = w.datas.Write(w.currBlock)
			if err != nil {
				return 0, err
			}
			currLen = 0
		} else if currLen == w.inode.Length {
			fmt.Printf("adding to end of current block\n")
		}
		// extend currBlock to min(currLen + len, BLOCKLENGTH)
		maxAddedByte := BLOCKLENGTH - (currLen)
		if uint64(length) > maxAddedByte {
			length = uint32(maxAddedByte)
		}
		w.currBlock, err = w.names.ExtendBlock(w.inode.Inodeid, w.currBlock.Id, length)
		if err != nil {
			return 0, err
		}
		// patch up local model
		w.inode.Length += uint64(length)
	} else {
		// find existing block we are overwriting
		newBlk, err := blockForPos(off, w.inode)
		if err != nil {
			return 0, err
		}
		fmt.Printf("block for pos %+v\n", w.currBlock)
		if newBlk.Id != w.currBlock.Id {
			// switch blocks
			w.currBlock = newBlk
		}
		if w.currBlock.EndPos-w.currBlock.StartPos < uint64(length) {
			// block isn't long enough, need to extend and/or curtail write
			// extend currBlock to min(currLen + len, BLOCKLENGTH)
			currLen := w.currBlock.EndPos - w.currBlock.StartPos
			maxAddedByte := BLOCKLENGTH - (currLen)
			if uint64(length) > maxAddedByte {
				length = uint32(maxAddedByte)
			}
			w.currBlock, err = w.names.ExtendBlock(w.inode.Inodeid, w.currBlock.Id, length)
			if err != nil {
				return 0, err
			}
			// patch up local model
			w.inode.Length += uint64(length)
		}
	}
	// if writer isn't configured to this block, switch it
	if w.currWriter == nil || w.currWriter.BlockId() != w.currBlock.Id {
		// force any pending changes
		if w.currWriter != nil {
			w.currWriter.Close()
		}
		fmt.Printf("allocating writer for block %d inode %d\n", w.currBlock.Id, w.inode.Inodeid)
		w.currWriter, err = w.datas.Write(w.currBlock)
		if err != nil {
			return 0, err
		}
	}
	// now write pages
	written = uint32(0)
	// write first page if fragment
	pageNum := int(off / uint64(4096))
	firstPageOff := off & (uint64(4095)) // fast modulo 4096
	if firstPageOff != 0 {
		firstPageLength := int(4096) - int(firstPageOff)
		err = w.currWriter.Write(p, pageNum, 0, firstPageLength)
		if err != nil {
			return 0, err
		}
		written += uint32(firstPageLength)
		pageNum++
	}

	// write whole pages into output buffer
	numWholePagesToWrite := (length - written) / PAGESIZE
	for ; numWholePagesToWrite > 0; numWholePagesToWrite-- {
		fmt.Printf("writing page to block %+v\n", w.currBlock)
		err = w.currWriter.WritePage(p[written:written+PAGESIZE], pageNum)
		if err != nil {
			return written, err
		}
		pageNum++
		written += PAGESIZE
	}

	// write last page if fragment
	if written < length {
		lastPageLen := int(length - written)

		fmt.Printf("writing last page len %d \n", lastPageLen)
		fmt.Printf("writing from slice %v at offsets %d %d\n", p, written, written+uint32(lastPageLen)-1)
		err = w.currWriter.Write(p[written:], pageNum, int(written), int(lastPageLen))
		written += uint32(lastPageLen)
		if err != nil {
			return written, err
		}
	}
	// update block with modified time
	w.currBlock.Mtime = uint64(time.Now().Unix())
	return written, nil
}

func (w *Writer) Fsync() (err error) {
	// update namenode with our inode's status
	if w.names == nil {
		fmt.Println("w.names nil wtf man")
	}
	newNode, err := w.names.Mutate(w.inode.Inodeid, func(i *maggiefs.Inode) error {
		i.Length = w.inode.Length
		currTime := time.Now().Unix()
		i.Mtime = currTime
		i.Ctime = currTime
		for idx, b := range w.inode.Blocks {
			if idx < len(i.Blocks) {
				i.Blocks[idx] = b
			} else {
				i.Blocks = append(i.Blocks, b)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	w.inode = newNode
	return nil
}

func (f *Writer) Close() (err error) {
	return nil
}
