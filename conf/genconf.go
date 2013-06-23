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
	for i := 0 ; i < 4 ; i++ {
		
	}
}




