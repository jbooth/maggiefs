package nameserver

import (
	"net/http"
	"time"
	"fmt"
	"strconv"
	"encoding/json"
)

func newNameWebServer(ns *NameServer, addr string) *http.Server {
  myHandler := http.NewServeMux()
  myHandler.HandleFunc("/inode",handle(ns.webInodeJson))  
  myHandler.HandleFunc("/stat",handle(ns.webFsStat))
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
func handle(f func (w http.ResponseWriter, r *http.Request) (error)) func (w http.ResponseWriter, r *http.Request) {
  return func(w http.ResponseWriter, r *http.Request) {
    err := f(w,r)
    if err != nil {
      fmt.Printf("web error evaluating func : %s",err.Error())
      w.Write([]byte(err.Error()))
    }
  }
}

// http methods
// render inode as json
func (ns *NameServer) webInodeJson(w http.ResponseWriter, r *http.Request) error {
  inodeid,err := strconv.ParseUint(r.Form.Get("inodeid"), 10, 64)
  if err != nil {
    return fmt.Errorf("webInodeJson: err parsing inode: %s",err.Error())
  }
  ino,err := ns.nd.GetInode(uint64(inodeid))
  if err != nil {
    return fmt.Errorf("webInodeJson: err getting inode id %d : %s",inodeid,err.Error())
  }
  w.Header().Set("Content-Type","application/json")
  bytes,err := json.Marshal(ino)
  if err != nil { return err }
  _,err = w.Write(bytes)
  return err
}

// show filesystem info
func (ns *NameServer) webFsStat(w http.ResponseWriter, r *http.Request) error {
  w.Header().Set("Content-Type","application/html")
  _,err := w.Write([]byte("STATS"))
  return err
}

