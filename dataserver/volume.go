package dataserver

import (
  "os"
  "github.com/jmhodges/levigo"
)

// represents a volume

// each volume has a root path, and then the directories 'meta' and 'blocks'
type volume struct {
  id int32
  rootPath string
  blockData *levigo.DB
}



func (v *volume) withFile(id uint64, op func (*os.File)) (error) {
  
  return nil
}

// paths are resolved using an intermediate hash so that we don't blow up the data dir with millions of entries
// we encode the ID into base64 and then take the first 2 bytes, limiting the top dir to 4096 entries
// rootPath / 
func (v *volume) resolvePath(blockid uint64) (string,error) {
  
}
