package dataserver

import (
	"encoding/json"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"net/http"
	"strconv"
	"time"
)

func newDataWebServer(ds *DataServer, addr string) *http.Server {
	myHandler := http.NewServeMux()
	myHandler.HandleFunc("/inode", handle(ds.webInodeJson))
	myHandler.HandleFunc("/blockLocations", handle(ds.getBlockLocations))
	http := &http.Server{
		Addr:           addr,
		Handler:        myHandler,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	return http
}

// wraps a func taking nameserver to a standard http handler
func handle(f func(w http.ResponseWriter, r *http.Request) error) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		err := f(w, r)
		if err != nil {
			fmt.Printf("web error evaluating func : %s", err.Error())
			w.Write([]byte(err.Error()))
		}
	}
}

// http methods
// render inode as json
func (ds *DataServer) webInodeJson(w http.ResponseWriter, r *http.Request) error {
	inodeid, err := strconv.ParseUint(r.FormValue("inodeid"), 10, 64)
	if err != nil {
		return fmt.Errorf("webInodeJson: err parsing inode: %s", err.Error())
	}
	ino, err := ds.ns.GetInode(uint64(inodeid))
	if err != nil {
		return fmt.Errorf("webInodeJson: err getting inode id %d : %s", inodeid, err.Error())
	}
	w.Header().Set("Content-Type", "application/json")
	bytes, err := json.Marshal(ino)
	if err != nil {
		return err
	}
	_, err = w.Write(bytes)
	return err
}

// given a relative path and offset into the file, get the locations for that offset
func (ds *DataServer) getBlockLocations(w http.ResponseWriter, r *http.Request) error {
	filePath := r.FormValue("file")
	start, err := strconv.ParseUint(r.FormValue("start"), 10, 64)
	if err != nil {
		return fmt.Errorf("getBlockLocations: err parsing start: %s", err.Error())
	}
	length, err := strconv.ParseUint(r.FormValue("length"), 10, 64)
	if err != nil {
		return fmt.Errorf("getBlockLocations: err parsing length: %s", err.Error())
	}
	
	ino, err := maggiefs.ResolveInode(filePath, ds.ns)
	if err != nil {
		return err
	}
	blocksInRange := maggiefs.BlocksInRange(ino.Blocks,start,length)
	if len(blocksInRange) == 0 {
		return fmt.Errorf("Offset %d not in any blocks on inode %+v", start, ino)
	}
	ret := make([]BlockLocation,len(blocksInRange))
	for blockIdx,b := range blocksInRange {
		item := BlockLocation{}
		item.Offset = int64(b.StartPos)
		item.Length = int64(b.EndPos - b.StartPos)
		item.Hosts = make([]string,len(b.Volumes))
		item.Names = make([]string,len(b.Volumes))
		item.TopologyPaths = make([]string,len(b.Volumes))
		
		for hostIdx,v := range b.Volumes {
			addr,err := ds.dc.VolHost(v)
			if err != nil {
				return err
			}
			item.Hosts[hostIdx] = addr.IP.String()
			item.Names[hostIdx] = addr.IP.String() + ":" + strconv.Itoa(addr.Port)
			item.TopologyPaths[hostIdx] = "/default-rack/" + addr.IP.String() + ":" + strconv.Itoa(addr.Port)
		}
		ret[blockIdx] = item
	}
	w.Header().Set("Content-Type", "application/json")
	bytes, err := json.Marshal(ret)
	if err != nil {
		return err
	}
	_, err = w.Write(bytes)
	return err
}

type BlockLocation struct {
	Hosts []string `json:"hosts"` // hostnames of peers
	Names []string `json:"names"` // hostname:port of peers
	TopologyPaths []string `json:"topologyPaths"` // full path name in network topology, unused
	Offset int64 `json:"offset"`
	Length int64 `json:"length"` 
}
