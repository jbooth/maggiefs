package maggiefs

import (
	"net"
)

type DataService interface {
	// given a volume ID (see struct Block), get the associated hostname
	// exposed for hadoop integration
	VolHost(volId uint32) (*net.TCPAddr, error)

	// read some bytes statelessly
	Read(blk Block, buf SplicerTo, pos uint64, length uint32) (err error)

	// executes an async write to the provided block, replicating to each volume in the order specified on the block
	// when done, onDone will be called
	Write(blk Block, p []byte, posInBlock uint64, onDone func()) (err error)
}

// represents one half of a pipe for splice-based communication with fuse, implemented by fuse.ReadPipe
// or a buffer on non-fuse-supporting platforms (unimplemented)
type SplicerTo interface {
	// write the header prior to splicing any bytes -- code should be 0 on OK, or a syscall value like syscall.EIO on error
	WriteHeader(code int32, returnBytesLength int) error
	// splice bytes from the FD to the return buffer
	SpliceBytes(fd uintptr, length int) (int, error)
	// splice bytes from the FD at the given offset to the return buffer
	SpliceBytesAt(fd uintptr, length int, offset int64) (int, error)
	// write bytes to the pipe from an in-memory buffer
	WriteBytes(b []byte) (int, error)
}

// interface exposed from datanodes to namenode (and tests)
type NameDataIface interface {
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
