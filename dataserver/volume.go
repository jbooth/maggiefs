package dataserver

import (
  "encoding/json"
  "encoding/binary"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jmhodges/levigo"
	"io/ioutil"
	"os"
	"syscall"
	"strconv"
)

var (
	readOpts  = levigo.NewReadOptions()
	writeOpts = levigo.NewWriteOptions()
	openOpts  = levigo.NewOptions()
)

func init() {
	openOpts.SetCreateIfMissing(true)
	writeOpts.SetSync(true)
}

func validVolume(volRoot string) bool {
	// check valid volid
	volid, err := getVolId(volRoot)
	if err != nil || volid < 1 {
		return false
	}
	// check valid metadb
	db, err := levigo.Open(volRoot+"/meta", openOpts)
	defer db.Close()
	if err != nil {
		return false
	}
	return true
}

func loadVolume(volRoot string) (*volume, error) {
	id, err := getVolId(volRoot)
	if err != nil {
		return nil, err
	}
	dnInfoFile,err := os.Open(volRoot + "/DNINFO")
  defer dnInfoFile.Close()
  if err != nil {
    return nil,err
  }
  d := json.NewDecoder(dnInfoFile)
  dnInfo := maggiefs.DataNodeInfo{}
  d.Decode(dnInfo)
  
  
	db, err := levigo.Open(volRoot+"/meta", openOpts)
	if err != nil {
		db.Close()
		return nil, err
	}
	
  rootFile,err := os.Open(volRoot)
  if err != nil { return nil,err }
	return &volume{id, volRoot, rootFile, maggiefs.VolumeInfo{id, dnInfo}, db}, nil
}

func formatVolume(volRoot string, vol maggiefs.VolumeInfo) (*volume, error) {
	// write vol id
	volIdFile, err := os.Create(volRoot + "/VOLID")
	defer volIdFile.Close()
	if err != nil {
		return nil, err
	}
	idBytes := []byte(strconv.Itoa(int(vol.VolId)))
	_, err = volIdFile.Write(idBytes)
	if err != nil {
		return nil, err
	}
	dnInfoFile, err := os.Create(volRoot + "/DNINFO")
	defer dnInfoFile.Close()
	if err != nil {
		return nil, err
	}
  e := json.NewEncoder(dnInfoFile)
  e.Encode(vol.DnInfo)
  
	// initialize db
	err = os.RemoveAll(volRoot + "/meta.ldb")
	if err != nil {
		return nil, err
	}
	db, err := levigo.Open(volRoot+"/meta.ldb", openOpts)
	if err != nil {
		return nil, err
	}
	// create block dir and subdirs
	err = os.RemoveAll(volRoot + "/blocks")
	if err != nil {
		return nil, err
	}
	err = os.Mkdir(volRoot+"/blocks", 0755)
	if err != nil {
		return nil, err
	}
	rootFile,err := os.Open(volRoot)
	if err != nil { return nil,err }
	return &volume{vol.VolId, volRoot, rootFile, vol, db}, nil
}

func getVolId(volRoot string) (int32, error) {
	bytes, err := ioutil.ReadFile(volRoot + "/VOLID")
	if err != nil {
		return -1, err
	}
	ret, err := strconv.Atoi(string(bytes))
	return int32(ret), err
}

// represents a volume
// each volume has a root path, with a file VOLID containing the string representation of our volume id,
// and then the directories 'meta' and 'blocks' which contain, respectively, a levelDB of block metadata 
// and the physical blocks
type volume struct {
	id        int32
	rootPath  string
	rootFile *os.File
	info      maggiefs.VolumeInfo
	blockData *levigo.DB
}

func (v *volume) HeartBeat() (stat maggiefs.VolumeStat, err error) {
  sysstat := syscall.Statfs_t{}
  err = syscall.Fstatfs(int(v.rootFile.Fd()),&sysstat)
  if err != nil { return stat,err }
  stat.VolumeInfo = v.info
  // these sizes are in blocks of 512
  stat.Size = sysstat.Blocks * uint64(sysstat.Bsize)
  stat.Used = (uint64(sysstat.Blocks - sysstat.Bfree) * uint64(sysstat.Bsize))
  stat.Free = sysstat.Bfree * uint64(sysstat.Bsize)
  return stat,nil
}

func (v *volume) AddBlock(blk maggiefs.Block) error {
  // TODO should blow up here if blk already exists
  // create file representing block
  f,err := os.Create(v.resolvePath(blk.Id))
  if err != nil { return err }
  defer f.Close()
  // add to blockmeta db
  key := make([]byte,8)
  binary.LittleEndian.PutUint64(key,blk.Id)
  val := blk.ToBytes()
  err = v.blockData.Put(writeOpts,key,val)
  return err
}

func (v *volume) RmBlock(id uint64) error {
  // kill file
  err := os.Remove(v.resolvePath(id))
  if err != nil { return err }
  // kill meta entry
  
  key := make([]byte,8)
  binary.LittleEndian.PutUint64(key,id)
  err = v.blockData.Delete(writeOpts,key)
  return err
}

func (v *volume) TruncBlock(blk maggiefs.Block, newSize uint32) error {
  return v.withFile(blk.Id, func(f *os.File) error {
    return f.Truncate(int64(newSize))
  })
}

func (v *volume) BlockReport() ([]maggiefs.Block, error) {
  ret := make([]maggiefs.Block,0,0)
  it := v.blockData.NewIterator(readOpts)
  defer it.Close()
  it.SeekToFirst()
  for it = it ; it.Valid() ; it.Next() {
    blk := maggiefs.Block{}
    blk.FromBytes(it.Value())
    ret = append(ret,blk)
  }
  return ret,it.GetError()
}

// NameDataIFace methods
//  // periodic heartbeat with datanode stats so namenode can keep total stats and re-replicate
//  HeartBeat() (stat *DataNodeStat, err error)
//  // add a block to this datanode/volume
//  AddBlock(blk Block, volId int32) (err error)
//  // rm block from this datanode/volume
//  RmBlock(id uint64, volId int32) (err error)
//  // truncate a block
//  TruncBlock(blk Block, volId int32, newSize uint32) (err error)
//  // get the list of all blocks for a volume
//  BlockReport(volId int32) (blocks []Block, err error)

func (v *volume) withFile(id uint64, op func(*os.File) error) error {
	f,err := os.Open(v.resolvePath(id))
	defer f.Close()
	if err != nil { return err }
	return op(f)
}

// paths are resolved using an intermediate hash so that we don't blow up the data dir with millions of entries
// we take the id modulo 1024 and use that as a string
func (v *volume) resolvePath(blockid uint64) string {
	return fmt.Sprintf("%s/%d/%d.block", v.rootPath, blockid&1023, blockid)
}
