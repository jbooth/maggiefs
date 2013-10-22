package dataserver

import (
    "os"
    "sync"
    "time"
    "sort"
)

type pooledFile struct {
    f *os.File
    path string
    lastUsed int64
}
type FilePool struct {
    highWater int
    lowWater int
    files map[string] *pooledFile
    l *sync.RWMutex
}

func NewFilePool(highWater,lowWater int) *FilePool {
	return &FilePool{highWater,lowWater,make(map[string]*pooledFile),new(sync.RWMutex)}
}

func (fp *FilePool) WithFile(path string, f func(*os.File) error) error {
    fp.l.RLock()
    pooledFile,exists := fp.files[path]
    if exists {
       pooledFile.lastUsed = time.Now().Unix() // we writelock before ever reading this val
       defer fp.l.RUnlock()
       return f(pooledFile.f) 
    }
    fp.l.RUnlock()
    // didn't exist so open
    fhandle,err := os.OpenFile(path, os.O_RDWR, 0)
    if err != nil {
        return err
    }
    err = f(fhandle)
    if err != nil {
        return err // ok not to add file to pool here
    }
    err = fp.addFile(path,fhandle)
    return err 
}

func (fp *FilePool) addFile(path string, f *os.File) (err error) {
    fp.l.Lock()
    defer fp.l.Unlock()
    fp.files[path] = &pooledFile{f,path,time.Now().Unix()}
    if len(fp.files) > fp.highWater {
        err = fp.closeSome()
    }
    return
}

func (fp *FilePool) closeSome() error {
	// put them in a list
	var sorter fileSorter = make([]*pooledFile,len(fp.files))
	idx := 0
	for _,f := range(fp.files) {
		sorter[idx] = f
		idx++
	}
	// sort them
	sort.Sort(sorter)	
	// close the ones we closin
	// sort is in increasing order so the first numToDelete
	numToDelete := len(fp.files) - fp.lowWater
	idx = 0
	for idx < numToDelete {
		f := sorter[idx]
		delete(fp.files,f.path)
		err := f.f.Close()
		if err != nil { 
			return err
		}
	}
	return nil
}

type fileSorter []*pooledFile

func (f fileSorter) Len() int { return len(f) }
func (f fileSorter) Less(i,j int) bool { return (f[i].lastUsed < f[j].lastUsed) }
func (f fileSorter) Swap(i,j int) { fi := f[i]; f[i] = f[j]; f[j] = fi; }