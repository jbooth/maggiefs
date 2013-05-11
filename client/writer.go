package client

import (
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

func (w *InodeWriter) Truncate(length uint64) error {
	// calls out to name service to truncate this file by repeatedly shrinking blocks
	return nil
}

//io.InodeWriter
func (w *InodeWriter) WriteAt(p []byte, off uint64, length uint32) (written uint32, err error) {

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
	}
	// now write bytes
	endOfWritePos := off + uint64(length)
	nWritten := 0
	for _, b := range inode.Blocks {
		if b.StartPos < off || b.EndPos > endOfWritePos {
			posInBlock := uint64(0)
			if b.StartPos < off {
				posInBlock += off - b.StartPos
			}
			writeLength := len(p)
			if b.EndPos > endOfWritePos {
				writeLength -= int(b.EndPos - endOfWritePos)
			}
			startIdx := nWritten
			endIdx := startIdx + writeLength
			err = w.datas.Write(b, p[startIdx:endIdx], posInBlock)
			if err != nil {
				return 0, err
			}
			nWritten += writeLength
		}
	}
	return uint32(nWritten), err
}

// acquires lease, then adds the blocks to the namenode,
// patching up the referenced inode to match
func (w *InodeWriter) addBlocksForFileWrite(inode *ino.Inode, off uint64, length uint32) error {
	// pick up lease
	lease, err := w.leases.WriteLease(w.inodeid)
	defer lease.Release()
	if err != nil {
		return err
	}
	newEndPos := off + uint64(length)
	if newEndPos > inode.Length {
		// extend last block to max block length first
		lastBlock := inode.Blocks[len(inode.Blocks)]
		if lastBlock.Length() < BLOCKLENGTH {
			extendLength := BLOCKLENGTH - lastBlock.Length()
			if lastBlock.EndPos+extendLength > off+uint64(length) {
				// only extend as much as we need to
				extendLength = off + uint64(length) - lastBlock.EndPos
			}
			lastBlock.EndPos = lastBlock.EndPos + extendLength
			inode.Blocks[len(inode.Blocks)] = lastBlock
			inode.Length += extendLength
		}
		// and add new blocks as necessary
		for newEndPos >= inode.Length {
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
	err = w.names.SetInode(inode)
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
