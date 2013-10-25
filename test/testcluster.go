package test

import (
	"github.com/jbooth/maggiefs/integration"
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
	testCluster, err = integration.NewSingleNodeCluster(4,2,3,"/tmp/testcluster","",true)
	if err != nil {
		panic(err)
	}
}
