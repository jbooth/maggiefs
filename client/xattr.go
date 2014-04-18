package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"log"
	"net"
	"strings"
)

// inode in json format
func inoJson(ino *maggiefs.Inode) []byte {
	bytes, err := json.Marshal(ino)
	if err != nil {
		log.Printf("Error marshalling ino %+v : %s", ino, err)
		return make([]byte, 0, 0)
	}
	return bytes
}

// lists blocklocations in tab-delimited text format
// BLOCKSTARTPOS \t BLOCKENDPOS \t HOST1,HOST2,HOST3
func blocLocs(ino *maggiefs.Inode, datas maggiefs.DataService) []byte {
	buf := new(bytes.Buffer)
	for _, b := range ino.Blocks {
		hosts := make([]string, 0, 0)
		for _, h := range b.Volumes {
			volHost, err := datas.VolHost(h)
			if err != nil {
				continue
			}
			host, _, err := net.SplitHostPort(volHost.String())
			if err != nil {
				log.Printf("error splitting hostport from volhost %s : %s", volHost, err)
				continue
			}
			hosts = append(hosts, host)
		}
		hostStr := strings.Join(hosts, ",")
		endPos := b.EndPos
		if endPos > ino.Length {
			endPos = ino.Length
		}
		lineStr := fmt.Sprintf("%d\t%d\t%s\n", b.StartPos, endPos, hostStr)
		buf.WriteString(lineStr)
	}
	return buf.Bytes()
}
