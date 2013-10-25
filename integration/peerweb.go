package integration

import (
	"encoding/json"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"github.com/jbooth/maggiefs/mrpc"
	"net/http"
	"strconv"
	"time"
	"net"
	"sync"
)

var peerWebTypeCheck mrpc.Service = &PeerWebServer{}

func NewPeerWebServer(ns maggiefs.NameService, dc maggiefs.DataService, mountPoint string, addr string) (*PeerWebServer,error) {
	laddr,err := net.ResolveTCPAddr("tcp",addr)
	if err != nil {
		return nil,err
	}
	l,err := net.ListenTCP("tcp",laddr)
	if err != nil {
		return nil,err
	}
	ret := &PeerWebServer {
		ns,
		dc,
		mountPoint,
		l,
		nil,
		sync.NewCond(new(sync.Mutex)),
		false,
	}
	myHandler := http.NewServeMux()
	myHandler.HandleFunc("/inode", handle(ret.webInodeJson))
	myHandler.HandleFunc("/blockLocations", handle(ret.getBlockLocations))
	myHandler.HandleFunc("/mountPoint", handle(ret.getMountPoint))
	
	http := &http.Server{
		Addr:           addr,
		Handler:        myHandler,
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   10 * time.Second,
		MaxHeaderBytes: 1 << 20,
	}
	ret.http = http
	return ret,nil
}

type PeerWebServer struct {
	ns maggiefs.NameService
	dc maggiefs.DataService
	mountPoint string
	l  *net.TCPListener
	http *http.Server
	clos *sync.Cond
	closed bool
}

func (ws *PeerWebServer) Serve() error {
	return ws.http.Serve(ws.l)
}

func (ws *PeerWebServer) Close() error {
	ws.clos.L.Lock()
	defer ws.clos.L.Unlock()
	if ws.closed {
		return nil
	}
	err := ws.l.Close()
	ws.closed = true
	ws.clos.Broadcast()
	return err
}

func (ws *PeerWebServer) WaitClosed() error {
	ws.clos.L.Lock()
	for !ws.closed {
		ws.clos.Wait()
	}
	ws.clos.L.Unlock()
	return nil
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
func (ws *PeerWebServer) webInodeJson(w http.ResponseWriter, r *http.Request) error {
	inodeid, err := strconv.ParseUint(r.FormValue("inodeid"), 10, 64)
	if err != nil {
		return fmt.Errorf("webInodeJson: err parsing inode: %s", err.Error())
	}
	ino, err := ws.ns.GetInode(uint64(inodeid))
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


// spit out our mountpoint for hadoop clients to delegate to
func (ws *PeerWebServer) getMountPoint(w http.ResponseWriter, r *http.Request) error {
	_, err := w.Write([]byte(ws.mountPoint))
	return err
}


// given a relative path and offset into the file, get the locations for that offset
func (ws *PeerWebServer) getBlockLocations(w http.ResponseWriter, r *http.Request) error {
	filePath := r.FormValue("file")
	start, err := strconv.ParseUint(r.FormValue("start"), 10, 64)
	if err != nil {
		return fmt.Errorf("getBlockLocations: err parsing start: %s", err.Error())
	}
	length, err := strconv.ParseUint(r.FormValue("length"), 10, 64)
	if err != nil {
		return fmt.Errorf("getBlockLocations: err parsing length: %s", err.Error())
	}
	
	ino, err := maggiefs.ResolveInode(filePath, ws.ns)
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
			addr,err := ws.dc.VolHost(v)
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
