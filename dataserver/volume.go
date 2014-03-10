package dataserver

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jmhodges/levigo"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"syscall"
)

var (
	readOpts  = levigo.NewReadOptions()
	writeOpts = levigo.NewWriteOptions()
	openOpts  = levigo.NewOptions()
	ZERO_64KB = [65536]byte{}
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

func loadVolume(volRoot string, fp *FilePool) (*volume, error) {
	id, err := getVolId(volRoot)
	if err != nil {
		return nil, err
	}
	dnInfoFile, err := os.Open(volRoot + "/DNINFO")
	defer dnInfoFile.Close()
	if err != nil {
		return nil, err
	}
	d := json.NewDecoder(dnInfoFile)
	dnInfo := maggiefs.DataNodeInfo{}
	d.Decode(&dnInfo)
	//fmt.Printf("Loading existing volume id %d, dnInfo %+v\n",id,dnInfo)
	db, err := levigo.Open(volRoot+"/meta", openOpts)
	if err != nil {
		db.Close()
		return nil, err
	}

	rootFile, err := os.Open(volRoot)
	if err != nil {
		return nil, err
	}
	return &volume{id, volRoot, rootFile, maggiefs.VolumeInfo{id, dnInfo}, db, fp}, nil
}

func formatVolume(volRoot string, vol maggiefs.VolumeInfo, fp *FilePool) (*volume, error) {
	volIdPath := volRoot + "/VOLID"
	// write vol id
	volIdFile, err := os.Create(volIdPath)
	if err != nil {
		return nil, fmt.Errorf("Volume.format : Error trying to create file %s : %s", volIdPath, err.Error())
	} else {
		defer volIdFile.Close()
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
	rootFile, err := os.Open(volRoot)
	if err != nil {
		return nil, err
	}
	return &volume{vol.VolId, volRoot, rootFile, vol, db, fp}, nil
}

func getVolId(volRoot string) (uint32, error) {
	bytes, err := ioutil.ReadFile(volRoot + "/VOLID")
	if err != nil {
		return 0, err
	}
	ret, err := strconv.Atoi(string(bytes))
	return uint32(ret), err
}

// represents a volume
// each volume has a root path, with a file VOLID containing the string representation of our volume id,
// and then the directories 'meta' and 'blocks' which contain, respectively, a levelDB of block metadata
// and the physical blocks
type volume struct {
	id        uint32
	rootPath  string
	rootFile  *os.File
	info      maggiefs.VolumeInfo
	blockData *levigo.DB
	fp        *FilePool
}

func (v *volume) Close() {
	v.blockData.Close()
	v.rootFile.Close()
}

func (v *volume) HeartBeat() (stat maggiefs.VolumeStat, err error) {
	sysstat := syscall.Statfs_t{}
	err = syscall.Fstatfs(int(v.rootFile.Fd()), &sysstat)
	if err != nil {
		return stat, err
	}
	//fmt.Printf("HeartBeat for volume id %d, dnInfo %+v\n",v.id,v.info)
	stat.VolumeInfo = v.info
	// these sizes are in blocks of 512
	stat.Size = sysstat.Blocks * uint64(sysstat.Bsize)
	stat.Used = (uint64(sysstat.Blocks-sysstat.Bfree) * uint64(sysstat.Bsize))
	stat.Free = sysstat.Bfree * uint64(sysstat.Bsize)
	return stat, nil
}

func (v *volume) AddBlock(blk maggiefs.Block, fallocate bool) error {
	// TODO should blow up here if blk already exists
	// create file representing block
	f, err := os.Create(v.resolvePath(blk.Id))
	if err != nil {
		return fmt.Errorf("Error creating block for %d : %s\n", blk.Id, err.Error())
	}
	if fallocate {
		stat, err := f.Stat()
		if err != nil {
			return err
		}
		syscall.Fallocate(int(f.Fd()), uint32(stat.Mode()), 0, int64(blk.Length()))
	}
	defer f.Close()
	// add to blockmeta db
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, blk.Id)
	binSize := blk.BinSize()
	val := make([]byte, binSize)
	blk.ToBytes(val)
	err = v.blockData.Put(writeOpts, key, val)
	return err
}

func (v *volume) getBlock(id uint64) (blk maggiefs.Block, err error) {
	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, id)
	val, err := v.blockData.Get(readOpts, key)
	if err != nil {
		blk.FromBytes(val)
	}
	return
}

func (v *volume) RmBlock(id uint64) error {
	// kill file
	err := os.Remove(v.resolvePath(id))
	if err != nil {
		return err
	}
	// kill meta entry

	key := make([]byte, 8)
	binary.LittleEndian.PutUint64(key, id)
	err = v.blockData.Delete(writeOpts, key)
	return err
}

func (v *volume) TruncBlock(blk maggiefs.Block, newSize uint32) error {
	return v.withFile(blk.Id, func(f *os.File) error {
		return f.Truncate(int64(newSize))
	})
}

func (v *volume) BlockReport() ([]maggiefs.Block, error) {
	ret := make([]maggiefs.Block, 0, 0)
	it := v.blockData.NewIterator(readOpts)
	defer it.Close()
	it.SeekToFirst()
	for it = it; it.Valid(); it.Next() {
		blk := maggiefs.Block{}
		blk.FromBytes(it.Value())
		ret = append(ret, blk)
	}
	return ret, it.GetError()
}

func (v *volume) withFile(id uint64, op func(*os.File) error) error {
	path := v.resolvePath(id)
	return v.fp.WithFile(path, op)
}

func (v *volume) serveDirectRead(result maggiefs.SplicerTo, req *RequestHeader) (err error) {
	err = v.withFile(req.Blk.Id, func(file *os.File) error {
		//		fmt.Printf("Serving read to file %s\n", file.Name())
		// check off
		sendPos := int64(req.Pos)
		stat, _ := file.Stat()
		// send only the bytes we have, then we'll send zeroes for the rest
		sendLength := uint64(req.Length)
		zerosLength := 0
		fileSize := uint64(stat.Size())
		if uint64(sendPos)+sendLength > fileSize {

			sendLength = uint64(stat.Size() - sendPos)
			zerosLength = int(uint32(req.Length) - uint32(sendLength))
		}
		// send data
		//		fmt.Printf("splicing data from pos %d length %d\n", sendPos, sendLength)
		_, err = result.SpliceBytesAt(file.Fd(), int(sendLength), sendPos)
		if err != nil {
			return err
		}
		for zerosLength > 0 {
			// send some zeroes
			zerosSend := zerosLength
			if zerosSend > 65536 {
				zerosSend = 65536
			}
			sent, err := result.WriteBytes(ZERO_64KB[:zerosSend])
			if err != nil {
				return err
			}
			zerosLength -= sent
		}
		return nil
	})
	return err

}

func (v *volume) serveRead(client *os.File, req *RequestHeader) (err error) {
	err = v.withFile(req.Blk.Id, func(file *os.File) error {
		//		fmt.Printf("Serving read to file %s\n", file.Name())
		// check off
		resp := ResponseHeader{STAT_OK, req.Reqno}
		sendPos := int64(req.Pos)
		// TODO should detect eof instead of doing stat up front, could save a syscall
		// send header
		_, err := resp.WriteTo(client)
		if err != nil {
			return err
		}
		// send data
		log.Printf("sendfile data from pos %d length %d\n", sendPos, req.Length)
		var sent int
		sent, err = syscall.Sendfile(int(client.Fd()), int(file.Fd()), &sendPos, int(req.Length))
		if err != nil {
			log.Printf("Error in sendfile: %s", err)
			return err
		}
    // if we sent less than the full amount
    if sent < int(req.Length) {
      // check size
		  // send only the bytes we have, then we'll send zeroes for the rest -- this is to support sparse files
		  stat, _ := file.Stat()
      sendLength := int(req.Length)
      if sent > 0 {
        sendLength -= sent
      }
		  zerosLength := 0
		  fileSize := int64(stat.Size())
		  if sendPos+int64(sendLength) > fileSize {
			  log.Printf("file name %s, length %d less than sendLength %d", stat.Name(), stat.Size(), sendLength)
			  sendLength = int(stat.Size() - sendPos)
			  zerosLength = int(uint32(req.Length) - uint32(sendLength))
		  }
      // send remaining bytes
      for sent < sendLength {
        s,err := syscall.Sendfile(int(client.Fd()),int(file.Fd()), &sendPos,sendLength)
        if err != nil {
          log.Printf("Error in sendFile second try: %s",err)
          return err
        }
        if s > 0 {
          sendLength -= s
        }
      }
      // send remaining zeroes
		  for zerosLength > 0 {
			  log.Printf("Sending %d zeros", zerosLength)
			  zerosSend := zerosLength
			  if zerosSend > 65536 {
				  zerosSend = 65536
			  }
			  s, err := client.Write(ZERO_64KB[:zerosSend])
			  if err != nil {
				  return err
			  }
			  zerosLength -= s
		  }

    }
		return nil
	})
	if os.IsNotExist(err) {
		// above function didn't run cause file didn't exist, so return NOBLOCK from here
		resp := ResponseHeader{STAT_NOBLOCK, req.Reqno}
		_, err = resp.WriteTo(client)
	}
	return err
}

// paths are resolved using an intermediate hash so that we don't blow up the data dir with millions of entries
// we take the id modulo 1024 and use that as a string
func (v *volume) resolvePath(blockid uint64) string {
	os.Mkdir(fmt.Sprintf("%s/blocks/%d", v.rootPath, blockid&1023), 0755)
	return fmt.Sprintf("%s/blocks/%d/%d.block", v.rootPath, blockid&1023, blockid)
}
