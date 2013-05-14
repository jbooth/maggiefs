package test

import (
	"github.com/jbooth/maggiefs/integration"
	"os"
)

var (
	testCluster *integration.SingleNodeCluster
)

func initCluster() {
	os.RemoveAll("/tmp/testcluster")
	var err error
	testCluster, err = integration.NewSingleNodeCluster(4, 2, 3, "/tmp/testcluster")
	if err != nil {
		panic(err)
	}
}

func teardownCluster() {
	testCluster.Close()
	err := testCluster.WaitClosed()
	if err != nil {
		panic(err)
	}
	return
}