package dataserver

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jmhodges/levigo"
	"io/ioutil"
	"net"
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

func loadVolume(volRoot string) (*volume, error) {
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
	d.Decode(dnInfo)

	db, err := levigo.Open(volRoot+"/meta", openOpts)
	if err != nil {
		db.Close()
		return nil, err
	}

	rootFile, err := os.Open(volRoot)
	if err != nil {
		return nil, err
	}
	return &volume{id, volRoot, rootFile, maggiefs.VolumeInfo{id, dnInfo}, db}, nil
}

func formatVolume(volRoot string, vol maggiefs.VolumeInfo) (*volume, error) {
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
	return &volume{vol.VolId, volRoot, rootFile, vol, db}, nil
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
	stat.VolumeInfo = v.info
	// these sizes are in blocks of 512
	stat.Size = sysstat.Blocks * uint64(sysstat.Bsize)
	stat.Used = (uint64(sysstat.Blocks-sysstat.Bfree) * uint64(sysstat.Bsize))
	stat.Free = sysstat.Bfree * uint64(sysstat.Bsize)
	return stat, nil
}

func (v *volume) AddBlock(blk maggiefs.Block) error {
	fmt.Printf("Adding block to vol %d\n", v.id)
	// TODO should blow up here if blk already exists
	// create file representing block
	f, err := os.Create(v.resolvePath(blk.Id))
	if err != nil {
		return fmt.Errorf("Error creating block for %d : %s\n", blk.Id, err.Error())
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
	fmt.Printf("doing block report in volume %d\n", v.id)
	ret := make([]maggiefs.Block, 0, 0)
	it := v.blockData.NewIterator(readOpts)
	defer it.Close()
	it.SeekToFirst()
	for it = it; it.Valid(); it.Next() {
		blk := maggiefs.Block{}
		blk.FromBytes(it.Value())
		fmt.Printf("got block %+v\n", blk)
		ret = append(ret, blk)
	}
	return ret, it.GetError()
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
	f, err := os.OpenFile(v.resolvePath(id),os.O_RDWR,0)
	defer f.Close()
	if err != nil {
	  fmt.Printf("Err opening file: %s\n",err.Error())
		return err
	}
	fmt.Printf("operating on file : %s\n",f.Name())
	return op(f)
}

func (v *volume) serveRead(client *os.File, req RequestHeader) (err error) {
	fmt.Println("serving read")
	err = v.withFile(req.Blk.Id, func(file *os.File) error {
		fmt.Printf("Serving read to file %s\n",file.Name())
		// check off
		resp := ResponseHeader{STAT_OK}
		// write header
		fmt.Println("writing header")
		_, err = resp.WriteTo(client)
		// sendfile
		if err != nil {
			return err
		}
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
		fmt.Printf("sending data from pos %d length %d\n",sendPos,sendLength)
		err = SendFile(file, client, sendPos, int(sendLength))
		if err != nil {
			return err
		}
		for zerosLength > 0 {
			// send some zeroes
			zerosSend := zerosLength
			if zerosSend > 65536 {
				zerosSend = 65536
			}
			sent, err := client.Write(ZERO_64KB[:zerosSend])
			if err != nil {
				return err
			}
			zerosLength -= sent
		}
		return nil
	})
	if os.IsNotExist(err) {
		// above function didn't run cause file didn't exist, so return NOBLOCK from here
		resp := ResponseHeader{STAT_NOBLOCK}
		_, err = resp.WriteTo(client)
	}
	return err
}

func (v *volume) serveWrite(client *os.File, req RequestHeader, datas *DataClient) error {
	resp := ResponseHeader{STAT_OK}

	// should we check block.Version here?  skipping for now
	err := v.withFile(req.Blk.Id, func(file *os.File) error {
		fmt.Printf("vol %d pipelining write to following vol IDs %+v\n", v.id, req.Blk.Volumes)
		// remove ourself from pipeline remainder
		for idx, volId := range req.Blk.Volumes {
			if volId == v.id {
				if idx == 0 {
					  req.Blk.Volumes = req.Blk.Volumes[1:]
				} else {
					if idx < len(req.Blk.Volumes) {
						req.Blk.Volumes = append(req.Blk.Volumes[:idx], req.Blk.Volumes[idx+1:]...)
					} else {
						req.Blk.Volumes = req.Blk.Volumes[:idx]
					}
				}
				break
			}
		}
		fmt.Printf("volumes after removing self %+v\n", req.Blk.Volumes)
		// prepare to splice
		outOffset := int64(req.Pos)
		// check if we have other dataservers in pipeline
		if len(req.Blk.Volumes) > 0 {
			err := datas.withConn(pickVol(req.Blk.Volumes), func(d *connFile) error {
				// forward request minus us in the replication chain
				req.WriteTo(d.f)
				// tee/splice
				tees := []*os.File{d.f}
				err := SpliceAdv(client, nil, file, &outOffset, tees, int(req.Length))
				if err != nil {
					err = fmt.Errorf("Error splicing on volume %d : %s",v.id,err.Error())
				}
				// TODO read response back so we don't fuck up the sockets
				return err
			})
			return err
		}
		// just us, so splice to file and return
		err := SpliceAdv(client, nil, file, &outOffset, nil, int(req.Length))
		return err
	})
	
  fmt.Printf("vol %d returning resp %+v \n",v.id,resp)
	// send response
	resp.WriteTo(client)
	return err
}

func checkResponse(c *net.TCPConn) error {
	resp := ResponseHeader{}
	resp.ReadFrom(c)
	if resp.Stat == STAT_OK {
		return nil
	}
	return fmt.Errorf("Response stat not ok : %d", resp.Stat)

}

// paths are resolved using an intermediate hash so that we don't blow up the data dir with millions of entries
// we take the id modulo 1024 and use that as a string
func (v *volume) resolvePath(blockid uint64) string {
	os.Mkdir(fmt.Sprintf("%s/blocks/%d", v.rootPath, blockid&1023), 0755)
	return fmt.Sprintf("%s/blocks/%d/%d.block", v.rootPath, blockid&1023, blockid)
}
