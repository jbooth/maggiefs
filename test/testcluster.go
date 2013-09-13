package test

import (
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/conf"
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
	nncfg, dscfg, err := conf.NewConfSet2(4, 2, 3, "/tmp/testcluster")
	if err != nil {
		panic(err)
	}
	testCluster, err = integration.NewSingleNodeCluster(nncfg, dscfg, true)
	if err != nil {
		panic(err)
	}
	// plug in name cache for tests
	nc := client.NewNameCache(testCluster.Names, testCluster.Leases)
	testCluster.Names = nc
	testCluster.Leases = nc
}
