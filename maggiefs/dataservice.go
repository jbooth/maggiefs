package maggiefs

import (

)



type DataService interface {
  // read some bytes
  Read(blk Block, p []byte, pos uint64, length uint64) (err error)

  // write some bytes, extending block if necessary
  // updates generation ID on datanodes before returning
  // if generation id doesn't match prev generation id, we have an error
  Write(blk Block, p []byte, pos uint64) (err error)
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
