package nameserver

import (
	"github.com/jbooth/maggiefs/maggiefs"
	"sort"
	"sync"
	"fmt"
	//"time"
)

type replicationManager struct {
  replicationFactor uint32
  volumes map[uint32]*volume // maps volumes to their host (host is immutable for a volume, new volumes always get new IDs)
  l          *sync.RWMutex
}
// internal object representing live connection to DN
type volume struct {
	stat    maggiefs.VolumeStat
	conn    maggiefs.NameDataIface
	l       *sync.Mutex
}


// execute something against one of the datanodes while holding its lock
func (v *volume) withLock(f func (v *volume) error) error {
  v.l.Lock()
  defer v.l.Unlock()
  return f(v)
}

func (rm *replicationManager) addDn(dn maggiefs.NameDataIface) error {
	stat,err := dn.HeartBeat()
	if err != nil { return err }
	for _,volStat := range stat.Volumes {
		vol := volume{}
		vol.stat = volStat
		vol.conn = dn
		rm.volumes[vol.stat.VolId] = &vol
	}
	return nil
}


func newReplicationManager(replicationFactor uint32) *replicationManager {
  return &replicationManager{
  	replicationFactor: replicationFactor,
  	volumes: make(map[uint32]*volume),
  }
}

// note, doesn't actually use suggestedDN just yet
func (rm *replicationManager) volumesForNewBlock(suggestedDN *int32) (volumes []maggiefs.VolumeStat, err error) {
	rm.l.RLock()
	defer rm.l.RUnlock()
	
	
	var sortedVolumes volumeList = make([]maggiefs.VolumeStat, 0)

	for _,v := range rm.volumes {
		sortedVolumes = append(sortedVolumes, v.stat)
	}
	sort.Sort(sortedVolumes)
	added := uint32(0)
	addedDNs := make(map[uint32]bool)
	ret := make([]maggiefs.VolumeStat, rm.replicationFactor)
	for i := 0; i < len(sortedVolumes); i++ {
		// check if this DN is in our added list
		v := sortedVolumes[i]
		if _, alreadyAdded := addedDNs[v.DnInfo.DnId]; alreadyAdded {
			// continue
		} else {
			// if not, add and increment added count
			ret[int(added)] = v
			added++
			addedDNs[v.DnInfo.DnId] = true
		}
		if added == rm.replicationFactor {
			break
		}
	}
	if added < rm.replicationFactor {
		return nil,fmt.Errorf("Not enough datanodes available for replication factor %d -- only nodes available were %+s",rm.replicationFactor,ret)
	}
	return ret,nil
}

func (rm *replicationManager) AddBlock(blk maggiefs.Block) error {
	for _,volId := range blk.Volumes {
		err := rm.volumes[volId].conn.AddBlock(blk,volId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rm *replicationManager) RmBlock(blk maggiefs.Block) error {
	for _,volId := range blk.Volumes {
		err := rm.volumes[volId].conn.RmBlock(blk.Id,volId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rm *replicationManager) TruncBlock(blk maggiefs.Block, newLength uint32) error {
		for _,volId := range blk.Volumes {
		err := rm.volumes[volId].conn.TruncBlock(blk,volId,newLength)
		if err != nil {
			return err
		}
	}
	return nil
}

type volumeList []maggiefs.VolumeStat

func (s volumeList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s volumeList) Len() int           { return len(s) }
func (s volumeList) Less(i, j int) bool { return s[i].Free < s[j].Free }

func (rm *replicationManager) cleanupDN(dnId uint32) error {
	return nil
}
