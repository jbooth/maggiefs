package maggiefs

import (
	"net"
)

// provides read and write capability to blocks, as well as a utility for displaying their locations
type DataService interface {
	// given a volume ID (see struct Block), get the associated hostname
	// exposed to support getattr blocklocations
	VolHost(volId uint32) (*net.TCPAddr, error)

	// we have 2 methods to read, in order to optimize by avoiding a context switch for singleblock reads
	// when done, onDone will be called from a dedicated goroutine (so don't block on anything)
	// onDone will be called with nil on success, err on error
	Read(blk Block, buf SplicerTo, posInBlock uint64, length uint32, onDone func(error)) error

	// executes a long read, opening a new socket and returning a persistent handle to it
	LongRead(blk Block, posInBlock uint64, length uint32) (BlockReader, error)

	// executes an async write to the provided block, replicating to each volume in the order specified on the block
	// when done, onDone will be called from a dedicated goroutine (so don't block on anything)
	// onDone will be called with nil on success, err on error
	Write(blk Block, p []byte, posInBlock uint64, onDone func(error)) (err error)
}

// represents one half of a pipe* for splice-based communication with fuse, implemented by fuse.ReadPipe
//
// * or a circular buffer on non-splice-supporting platforms (unimplemented)
type SplicerTo interface {
	// splice bytes from the FD to this buffer
	LoadFrom(fd uintptr, length int) (int, error)
	// splice bytes from the FD at the given offset to this buffer
	LoadFromAt(fd uintptr, length int, offset int64) (int, error)
	// write bytes to the pipe from an in-memory buffer
	Write(b []byte) (int, error)
}

// represents a persistent read handle for reads that are longer than a single buffer
type BlockReader interface {
	SpliceTo(p SplicerTo, length uint32) (nSpliced int, err error)
	Close() error
}

// interface exposed from peers to master for administration (and tests)
// client never interacts with peer interface directly
type Peer interface {
	// periodic heartbeat with datanode stats so namenode can keep total stats and re-replicate
	HeartBeat() (stat *DataNodeStat, err error)
	// add a block to this datanode/volume
	AddBlock(blk Block, volId uint32, fallocate bool) (err error)
	// rm block from this datanode/volume
	RmBlock(id uint64, volId uint32) (err error)
	// truncate a block
	TruncBlock(blk Block, volId uint32, newSize uint32) (err error)
	// get the list of all blocks for a volume
	BlockReport(volId uint32) (blocks []Block, err error)
}
