package test

import (
	"fmt"
	"github.com/jbooth/maggiefs/client"
	"github.com/jbooth/maggiefs/integration"
	"os"
)

var (
	testCluster *integration.SingleNodeCluster
	openFiles   *client.OpenFileMap
)

func init() {
	initCluster()
	go func() {
		fmt.Printf("Starting test cluster\n")
		testCluster.Serve()
	}()
}

func initCluster() {
	os.RemoveAll("/tmp/testcluster")
	var err error
	//testCluster, err = integration.NewSingleNodeCluster(4, 2, 3, "/tmp/testcluster", "", true)
	testCluster, err = integration.NewSingleNodeCluster(11004, 1103, 4, 2, 3, "/tmp/testcluster", "", true)
	if err != nil {
		panic(err)
	}
	openFiles = client.NewOpenFileMap(testCluster.Leases, testCluster.Names, testCluster.Datas, nil, func(i uint64) {})
}
