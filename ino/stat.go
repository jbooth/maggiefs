package maggiefs

import (
	"net"
	"reflect"
)

type VolDnMap map[int32]*net.Addr

type DataNodeInfo struct {
	DnId uint32
	Addr string // includes port in colon format
}

func (dni DataNodeInfo) Equals(other DataNodeInfo) bool {
	return reflect.DeepEqual(dni, other)
}

type VolumeInfo struct {
	VolId  uint32
	DnInfo DataNodeInfo
}

type FsStat struct {
	Size              uint64
	Used              uint64
	Free              uint64
	ReplicationFactor uint32
	DnStat            []DataNodeStat
}

type DataNodeStat struct {
	DataNodeInfo
	Volumes []VolumeStat
}

func (dn DataNodeStat) Size() uint64 {
	ret := uint64(0)
	for _, v := range dn.Volumes {
		ret += v.Size
	}
	return ret
}

func (dn DataNodeStat) Used() uint64 {
	ret := uint64(0)
	for _, v := range dn.Volumes {
		ret += v.Used
	}
	return ret
}

func (dn DataNodeStat) Free() uint64 {
	ret := uint64(0)
	for _, v := range dn.Volumes {
		ret += v.Free
	}
	return ret
}

type VolumeStat struct {
	VolumeInfo
	Size uint64 // total bytes
	Used uint64 // bytes used
	Free uint64 // bytes free
}
