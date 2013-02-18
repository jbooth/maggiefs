package nameserver

import (
	"github.com/jbooth/maggiefs/maggiefs"
	"net"
	"sort"
	"sync"
	//"time"
)

// internal object representing live connection to DN
type volume struct {
  volid   uint32
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

type replicationManager struct {
  replicationFactor uint32
	volumes map[int32]*volume // maps volumes to their host (host is immutable for a volume, new volumes always get new IDs)
	conns map[int32] maggiefs.NameDataIface
	hosts map[uint32]*maggiefs.DataNodeStat
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

// ensures that all datanodes see the current version of the given block
// expects block.Volumes to be filled out correctly, those are the DNs we notify
func (rm *replicationManager) replicate(b *maggiefs.Block) (error) {
  rm.l.Lock()
  defer rm.l.Unlock()
  
  for _,volId := range b.Volumes {
    vol := rm.volumes[volId]
    
  }
  
  
  
  // allocate actual blocks on datanodes and update dn stats
  for _,h := range  rm.volumeHost {
    err := h.withLock(
      func(d *dnHost) error {
        return h.conn.AddBlock(b.Id)
      })
    if err != nil {
      return err
    }
  }  
	return nil
}

// note, doesn't actually use suggestedDN just yet
func (rm *replicationManager) volumesForNewBlock(suggestedDN *int32) (volumes []maggiefs.VolumeStat) {
	rm.l.RLock()
	defer rm.l.RUnlock()
	
	
	var sortedVolumes volumeList = make([]maggiefs.VolumeStat, 0)

	for _,h := range rm.hosts {
		sortedVolumes = append(sortedVolumes, h.Volumes...)
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
