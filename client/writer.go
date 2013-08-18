package client

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"time"
)

type InodeWriter struct {
	inodeid uint64
	leases  maggiefs.LeaseService
	names   maggiefs.NameService
	datas   maggiefs.DataService
}

func NewInodeWriter(inodeid uint64, leases maggiefs.LeaseService, names maggiefs.NameService, datas maggiefs.DataService) (w *InodeWriter, err error) {
	if err != nil {
		return nil, err
	}

	return &InodeWriter{inodeid, leases, names, datas}, nil
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
func (w *InodeWriter) WriteAt(p []byte, off uint64, length uint32) (written uint32, err error) {
	// pick up lease
	// TODO we could release this earlier and get better throughput, need a way to guarantee atomicity though
	lease, err := w.leases.WriteLease(w.inodeid)
	defer lease.Release()
	if err != nil {
		return 0, err
	}

	inode, err := w.names.GetInode(w.inodeid)
	if err != nil {
		return 0, err
	}
	// check if we need to add blocks, note we support sparse files
	// so it's ok to add blocks that won't be written to right away,
	// on read they'll return 0 for all bytes in empty sections, and
	// won't read past
	if off+uint64(length) > inode.Length {
		err = w.addBlocksForFileWrite(inode, off, length)
		if err != nil {
			return 0, err
		}
  	//fmt.Printf("Added blocks for filewrite to ino %+v\n", inode)
	}
	// now write bytes
	nWritten := 0
	endOfWritePos := off + uint64(length) - 1
  startOfWritePos := off + uint64(nWritten)
	for _, b := range inode.Blocks {
		//fmt.Printf("evaluating block %+v for writeStartPos %d endofWritePos %d\n", b, startOfWritePos, endOfWritePos)
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
			err = w.datas.Write(b, p[startIdx:endIdx], posInBlock)
			if err != nil {
				return 0, err
			}
//			fmt.Printf("Wrote %d bytes to block %+v\n", endIdx-startIdx, b)
			nWritten += writeLength
//			fmt.Printf("Wrote %d, nWritten total %d", writeLength, nWritten)
		}
	}
	return uint32(nWritten), err
}

// acquires lease, then adds the blocks to the namenode,
// patching up the referenced inode to match
func (w *InodeWriter) addBlocksForFileWrite(inode *maggiefs.Inode, off uint64, length uint32) error {
//	fmt.Printf("Adding/extending blocks for write at off %d length %d\n", off, length)
	newEndPos := off + uint64(length)
	if newEndPos > inode.Length {
		// if we have a last block and it's less than max length,
		// extend last block to max block length first
//		fmt.Printf("Adding/extending blocks for file write to inode %+v\n", inode)
		if inode.Blocks != nil && len(inode.Blocks) > 0 {
			idx := int(len(inode.Blocks)-1)
			lastBlock := inode.Blocks[idx]
			if lastBlock.Length() < BLOCKLENGTH {
				extendLength := BLOCKLENGTH - lastBlock.Length()
				if lastBlock.EndPos+extendLength > off+uint64(length) {
					// only extend as much as we need to
					extendLength = off + uint64(length) - lastBlock.EndPos
				}
				lastBlock.EndPos = lastBlock.EndPos + extendLength
				inode.Blocks[idx] = lastBlock
				inode.Length += extendLength
			}
		}
		// and add new blocks as necessary
		for newEndPos > inode.Length {
//			fmt.Printf("New end pos %d still greater than inode length %d\n", newEndPos, inode.Length)
			newBlockLength := newEndPos - inode.Length
			if newBlockLength > BLOCKLENGTH {
				newBlockLength = BLOCKLENGTH
			}
			newBlock, err := w.names.AddBlock(inode.Inodeid, uint32(newBlockLength))
			if err != nil {
				return err
			}
			inode.Blocks = append(inode.Blocks, newBlock)
			inode.Length += newBlockLength
		}
	}
	inode.Mtime = time.Now().Unix()
	err := w.names.SetInode(inode)
	return err
}

func (w *InodeWriter) Fsync() (err error) {
	l, err := w.leases.WriteLease(w.inodeid)
	if err != nil {
		return err
	}
	l.Release()
	return nil
}

func (f *InodeWriter) Close() (err error) {
	return nil
}
