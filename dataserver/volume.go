package dataserver

import (
  "encoding/json"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jmhodges/levigo"
	"io/ioutil"
	"os"
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
	
	return &volume{id, volRoot, maggiefs.VolumeInfo{id, dnInfo}, db}, nil
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
	return &volume{vol.VolId, volRoot, vol, db}, nil
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
	info      maggiefs.VolumeInfo
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
