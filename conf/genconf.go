//// generates config files in the working directory and makes dirs for data
//package main
//
//import (
//  "github.com/jbooth/maggiefs/integration"
//	"flag"
//	"fmt"
//  "os"
//)
//
//func main() {
//	
//	
//	flag.Parse()
//	args := flag.Args()
//	if len(args) < 1 {
//		fmt.Println("genconf confDir dataDir")
//		return
//	}
//	baseDir := args[0]
//  dataDir := args[1]
//  os.Mkdir(dataDir,0777)
//	nnbind :=  "0.0.0.0:12001"
//	lsbind := "0.0.0.0:12002"
//  nncfg := integration.NNConfig{}
//  fmt.Println(nncfg)
//  nncfg.NameBindAddr = nnbind
//  nncfg.LeaseBindAddr = lsbind
//  nnhome := fmt.Sprintf("%s/nnhome",dataDir)
//  nncfg.NNHomeDir = nnhome
//  os.Mkdir(nnhome,0777)
//  nncfg.ReplicationFactor = 2
//	err := nncfg.Write(fmt.Sprintf("%s/nn.cfg",baseDir))
//	if err != nil {
//		panic(err)
//	}
//	
//	dscfg := integration.DSConfig{}
//	port := 12101
//	for i := 0 ; i < 4 ; i++ {
//		dscfg.NameAddr = nnbind
//		dscfg.LeaseAddr = lsbind
//		dscfg.DataClientBindAddr = fmt.Sprintf("0.0.0.0:%d",port)
//		port++
//		dscfg.NameDataBindAddr = fmt.Sprintf("0.0.0.0:%d",port)
//		port += 100
//    volRoot1 := fmt.Sprintf("/home/jay/mfshome/data%d-0",i)
//    volRoot2 := fmt.Sprintf("/home/jay/mfshome/data%d-1",i)
//		dscfg.VolumeRoots = []string { volRoot1, volRoot2 }
//    os.Mkdir(volRoot1,0777)
//    os.Mkdir(volRoot2,0777)
//    dscfg.Write(fmt.Sprintf("%s/dn%d.cfg",baseDir,i))
//	}
//}
//
//
//
//
