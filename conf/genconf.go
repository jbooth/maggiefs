// generates these config files in the working directory
package main

import (
  "github.com/jbooth/maggiefs/integration"
	"flag"
	"fmt"
)

func main() {
	
	
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		fmt.Println("please supply argument for dir to write in")
		return
	}
	baseDir := args[0]
	nnbind :=  "0.0.0.0:12001"
	lsbind := "0.0.0.0:12002"
  nncfg := integration.NNConfig{}
  fmt.Println(nncfg)
  nncfg.NameBindAddr = nnbind
  nncfg.LeaseBindAddr = lsbind
  nncfg.NNHomeDir = "/tmp/maggiefs/nnhome"
  nncfg.ReplicationFactor = 2
	err := nncfg.Write(fmt.Sprintf("%s/nn.cfg",baseDir))
	if err != nil {
		panic(err)
	}
	
	dscfg := integration.DSConfig{}
	port := 12101
	for i := 0 ; i < 4 ; i++ {
		dscfg.NameAddr = nnbind
		dscfg.LeaseAddr = lsbind
		dscfg.DataClientBindAddr = fmt.Sprintf("0.0.0.0:%d",port)
		port++
		dscfg.NameDataBindAddr = fmt.Sprintf("0.0.0.0:%d",port)
		port += 100
		dscfg.VolumeRoots = []string { fmt.Sprintf("/tmp/data%d-0",i), fmt.Sprintf("/tmp/data%d-1",i) }
    dscfg.Write(fmt.Sprintf("%s/dn%d.cfg",baseDir,i))
	}
}




