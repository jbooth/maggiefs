package nameserver

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"sort"
	"sync"
	"time"
)

type replicationManager struct {
	replicationFactor uint32
	volumes           map[uint32]*volume // maps volumes to their host (host is immutable for a volume, new volumes always get new IDs)
	l                 *sync. RWMutex
	dnCheckers        map[uint32]*dnChecker // maps hostnames to the struct responsible for polling them
}

// internal object representing live connection to DN
type volume struct {
	stat maggiefs.VolumeStat
	conn maggiefs.NameDataIface
}


type dnChecker struct {
  quit chan bool
  dnInfo *maggiefs.DataNodeInfo
  dn maggiefs.NameDataIface
}

// monitors this volume, updating it's stats locally and notifying if DN dies
func (rm *replicationManager) monitorStat(c *dnChecker) {
  ticker := time.Tick(60 * time.Second)
  for {
    
    stat,err := c.dn.HeartBeat()
    if err != nil {
      // datanode is down, log error, invalidate volumes and return
      fmt.Println("DATANODE DOWN, NEED TO CODE VALID BEHAVIOR HERE")
      return
    }
    // update stats 
    rm.l.Lock()
    for _,volStat := range stat.Volumes {
      rm.volumes[volStat.VolId] = &volume{volStat,c.dn}
    }
    rm.l.Unlock()
    // wait 60 seconds
    select {
      case _ = <- c.quit:
        // quit signal, return
        return
      case _ = <- ticker:
        // keep on tickin
        continue
    }
  }
}

func (rm *replicationManager) dnDead() {
}

func (rm *replicationManager) addDn(dn maggiefs.NameDataIface) error {
	stat, err := dn.HeartBeat()
	if err != nil {
		return err
	}
	rm.l.Lock()
	// setup stats
	for _, volStat := range stat.Volumes {
		vol := volume{}
		vol.stat = volStat
		vol.conn = dn
		rm.volumes[vol.stat.VolId] = &vol
	}
	// start checker
	dnInfo := &maggiefs.DataNodeInfo{}
	dnInfo.DnId = stat.DnId
	dnInfo.Addr = stat.Addr
	
	checker := &dnChecker{make(chan bool,1), dnInfo, dn }
	rm.dnCheckers[dnInfo.DnId] = checker 
	rm.l.Unlock()
	
	return nil
}

func newReplicationManager(replicationFactor uint32) *replicationManager {
	return &replicationManager{
		replicationFactor: replicationFactor,
		volumes:           make(map[uint32]*volume),
		l:                 &sync.RWMutex{},
		dnCheckers:          make(map[uint32]*dnChecker),
	}
}

func (rm *replicationManager) Close() {
  rm.l.Lock()
  defer rm.l.Unlock()
  for _,ck := range rm.dnCheckers {
    ck.quit <- true
  }
}

func (rm *replicationManager) FsStat() (maggiefs.FsStat, error) {
	ret := maggiefs.FsStat{}
	ret.DnStat = make([]maggiefs.DataNodeStat, 0)
	rm.l.RLock()
	defer rm.l.RUnlock()
	dnStats := make(map[uint32]*maggiefs.DataNodeStat)
	for _, vol := range rm.volumes {
		_, ok := dnStats[vol.stat.DnInfo.DnId]
		if !ok {
			dns, err := vol.conn.HeartBeat()
			if err != nil {
				return ret, fmt.Errorf("Error getting heartbeat for dnid %d", vol.stat.DnInfo.DnId)
			}
			dnStats[vol.stat.DnInfo.DnId] = dns
			ret.DnStat = append(ret.DnStat, *dns)
			ret.Free += dns.Free()
			ret.Size += dns.Size()
			ret.Used += dns.Used()
		}
	}
	return ret, nil
}

// note, doesn't actually use suggestedDN just yet
func (rm *replicationManager) volumesForNewBlock(suggestedDN *int32) (volumes []maggiefs.VolumeStat, err error) {
	rm.l.RLock()
	defer rm.l.RUnlock()

	var sortedVolumes volumeList = make([]maggiefs.VolumeStat, 0)

	for _, v := range rm.volumes {
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
		return nil, fmt.Errorf("Not enough datanodes available for replication factor %d -- only nodes available were %+s", rm.replicationFactor, ret)
	}
	return ret, nil
}

func (rm *replicationManager) AddBlock(blk maggiefs.Block) error {
	for _, volId := range blk.Volumes {
		err := rm.volumes[volId].conn.AddBlock(blk, volId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rm *replicationManager) RmBlock(blk maggiefs.Block) error {
	for _, volId := range blk.Volumes {
		err := rm.volumes[volId].conn.RmBlock(blk.Id, volId)
		if err != nil {
			return err
		}
	}
	return nil
}

func (rm *replicationManager) TruncBlock(blk maggiefs.Block, newLength uint32) error {
	for _, volId := range blk.Volumes {
		err := rm.volumes[volId].conn.TruncBlock(blk, volId, newLength)
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
