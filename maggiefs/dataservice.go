package maggiefs

import (

)



type DataService interface {
  // read some bytes
  Read(blk Block, p []byte, pos uint64, length uint64) (err error)

  // write some bytes
  Write(blk Block, p []byte, pos uint64) (err error)
}

type BlockWriter interface {
  // return which block id this writer is writing
  BlockId() uint64
  // writes some bytes, extending block if necessary
  Write(p []byte, pos uint64) error
  // flushes changes to system
  Sync() (err error)
  // flushes and closes this writer
  Close() (err error)
}

// interface exposed from datanodes to namenode (and tests)
type NameDataIface interface {
  // periodic heartbeat with datanode stats so namenode can keep total stats and re-replicate
  HeartBeat() (stat *DataNodeStat, err error)
  // add a block to this datanode/volume
  AddBlock(blk Block, volId int32) (err error)
  // rm block from this datanode/volume
  RmBlock(id uint64, volId int32) (err error)
  // truncate a block
  TruncBlock(blk Block, volId int32, newSize uint32) (err error)
  // get the list of all blocks for a volume
  BlockReport(volId int32) (blocks []Block, err error)
}
