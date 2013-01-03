package nameserver

import (
	"github.com/jbooth/maggiefs/maggiefs"
	"net"
	"sort"
	"sync"
	"time"
)

type datanodeStat struct {
	conn maggiefs.NameDataIface
	stat maggiefs.DataNodeStat
	l    *sync.Mutex
}

type dnHost struct {
	id      uint32
	addr    net.Addr
	stat maggiefs.DataNodeStat
	conn    maggiefs.NameDataIface
	l       *sync.Mutex
}

// execute something against one of the datanodes while holding its lock
func (d *dnHost) withLock(f func (dn *dnHost) error) error {
  d.l.Lock()
  defer d.l.Unlock()
  return f(d)
}

type replicationManager struct {
  replicationFactor uint32
	volumeHost map[int32]int32 // maps volumes to their host
	hosts      map[int32]*dnHost
	l          *sync.RWMutex
}

func newReplicationManager() *replicationManager {
  return nil
}

func (rm *replicationManager) formatVolume() error {
  return nil
}

func (rm *replicationManager) addDN(c *net.TCPConn) error {
  // heartbeat to get DN stat
  
  // check for valid volumes
  
  // format any new volumes on offer
  return nil
}

// adds a block to the end of the given inode, incorporating the suggested DN if possible.
// updates state on provided NameData
func (rm *replicationManager) addBlock(inodeid uint64, blockid uint64, startPos uint64, suggestedDN *int32) (maggiefs.Block, error) {
  volumes := rm.volumesForNewBlock(suggestedDN)
  ret := maggiefs.Block{}
  ret.Id = blockid
  ret.Inodeid = inodeid
  ret.Volumes = make([]int32,rm.replicationFactor)
  for idx,v := range volumes {
    ret.Volumes[idx] = v.Id
  }
  ret.Mtime = time.Now().Unix()
  ret.StartPos = startPos
  ret.EndPos = startPos
  
  rm.l.Lock()
  defer rm.l.Unlock()
  // allocate actual blocks on datanodes and update dn stats
  for _,v := range volumes {
    err := rm.hosts[v.DnId].withLock(
      func(d *dnHost) error {
        return d.conn.AddBlock(blockid)
      })
    if err != nil {
      return maggiefs.Block{},err
    }
  }  
  
  
	return ret, nil
}

// note, doesn't actually use suggestedDN just yet
func (rm *replicationManager) volumesForNewBlock(suggestedDN *int32) (volumes []maggiefs.VolumeStat) {
	rm.l.RLock()
	defer rm.l.RUnlock()
	var sortedVolumes volumeList = make([]maggiefs.VolumeStat, 0)
	for i := 0; i < len(rm.hosts); i++ {
		sortedVolumes = append(sortedVolumes, rm.hosts[int32(i)].stat.Volumes...)
	}
	sort.Sort(sortedVolumes)
	added := uint32(0)
	addedDNs := make(map[int32]bool)
	ret := make([]maggiefs.VolumeStat, rm.replicationFactor)
	for i := 0; i < len(sortedVolumes); i++ {
		// check if this DN is in our added list
		v := sortedVolumes[i]
		if _, alreadyAdded := addedDNs[v.DnId]; alreadyAdded {
		} else {
			// if not, add and increment added count
			ret[int(added)] = v

		}
		if added == rm.replicationFactor {
			break
		}
	}
	return ret
}

type volumeList []maggiefs.VolumeStat

func (s volumeList) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s volumeList) Len() int           { return len(s) }
func (s volumeList) Less(i, j int) bool { return s[i].Free < s[j].Free }

func (rm *replicationManager) cleanupDN(dnId uint32) error {
	return nil
}
