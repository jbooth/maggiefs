package dataserver

import (
	"fmt"
	"github.com/jmhodges/levigo"
	"os"
	"io/ioutil"
	"strconv"
)

var (
  readOpts = levigo.NewReadOptions()
  writeOpts = levigo.NewWriteOptions()
  openOpts = levigo.NewOptions()
)

func init() {
  writeOpts.SetSync(true)
}

func validVolume(volRoot string) bool {
  // check valid volid
  volid,err := getVolId(volRoot)
  if err != nil || volid < 1 { 
    return false
  }
  // check valid metadb
  db,err := levigo.Open(volRoot + "/meta",openOpts)
  defer db.Close()
  if err != nil {
    return false
  } 
  return true
}

func loadVolume(volRoot string) (*volume, error) {
  id,err := getVolId(volRoot)
  if err != nil { return nil,err }
  db,err := levigo.Open(volRoot + "/meta",openOpts)
  if err != nil {
    db.Close()
    return nil,err
  } 
  return &volume{id,volRoot,db},nil
}

func formatVolume(volRoot string, volId int32) (*volume, error) {
  // write vol id
  
  // initialize db
  
  // create block dir and subdirs
}

func getVolId(volRoot string) (int32,error) {
  bytes,err := ioutil.ReadFile(volRoot + "/VOLID")
  if err != nil { return -1,err }
  ret,err := strconv.Atoi(string(bytes))
  return int32(ret),err
}

// represents a volume
// each volume has a root path, with a file VOLID containing the string representation of our volume id,
// and then the directories 'meta' and 'blocks' which contain, respectively, a levelDB of block metadata 
// and the physical blocks
type volume struct {
	id        int32
	rootPath  string
	blockData *levigo.DB
}

func (v *volume) withFile(id uint64, op func(*os.File)) error {

	return nil
}

// paths are resolved using an intermediate hash so that we don't blow up the data dir with millions of entries
// we take the id modulo 1024 and use that as a string
func (v *volume) resolvePath(blockid uint64) string {
	return fmt.Sprintf("%s/%d/%d.block", v.rootPath, blockid&1023, blockid)
}
