package test

import (
	"github.com/jbooth/maggiefs/integration"
	"github.com/jbooth/maggiefs/conf"
	"os"
)

var (
	testCluster *integration.SingleNodeCluster
)

func init() {
  initCluster()
}

func initCluster() {
	os.RemoveAll("/tmp/testcluster")
	var err error
	nncfg,dscfg,err := conf.NewConfSet2(4,2,3,"/tmp/testcluster")
	if err != nil {
	  panic(err)
	}
	testCluster, err = integration.NewSingleNodeCluster(nncfg,dscfg,true)
	if err != nil {
		panic(err)
	}
}
