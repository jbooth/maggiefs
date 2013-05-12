package integration

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"os"
	"testing"
)

var (
	testCluster *SingleNodeCluster
)

func initCluster() {
	os.RemoveAll("/tmp/testcluster")
	var err error
	testCluster, err = TestCluster(4, 2, 3, "/tmp/testcluster")
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

func TestAddInodeToCluster(t *testing.T) {
	fmt.Println("setting up cluster")
	initCluster()
	defer teardownCluster()
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.names.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	fmt.Println("getting node from cluster")
	ino2, err := testCluster.names.GetInode(id)
	if !ino.Equals(ino2) {
		t.Fatal(fmt.Errorf("Error, inodes not equal : %+v : %+v\n", *ino, *ino2))
	}

}

func TestAddBlock(t *testing.T) {
	initCluster()
	defer teardownCluster()
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id
	newBlock, err := testCluster.names.AddBlock(ino.Inodeid, 1024)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("got block back %+v\n", newBlock)
	ino, err = testCluster.names.GetInode(ino.Inodeid)

	if newBlock.Id != ino.Blocks[0].Id || ino.Blocks[0].EndPos != 1024 {
		t.Fatal(fmt.Errorf("Wrong end length for block %+v", ino.Blocks[0]))
	}
	// check that block made it to each datanode
	fstat, err := testCluster.names.StatFs()
	fmt.Printf("got fstat %+v\n", fstat)
	for _, dnInfo := range fstat.DnStat {
		for _, volStat := range dnInfo.Volumes {
			volId := volStat.VolId
			for _, blockVolId := range newBlock.Volumes {
				if blockVolId == volId {
					fmt.Printf("looking for vol %d on dn %d\n",volId,dnInfo.DnId)
					// dnIDs start at 1 so decrement 
					blocks, err := testCluster.dataNodes[dnInfo.DnId - 1].BlockReport(volId)
					if err != nil {
						t.Fatal(err)
					}
					fmt.Printf("Blocks for vol %d : %+v\n", volId, blocks)

					var found = false
					for _, blk := range blocks {
						if blk.Id == newBlock.Id {
							found = true
						}
					}
					if !found {
						t.Fatalf("Didn't find block %d on volume %d!  Blocks on volume: \n %+v \n", newBlock.Id, volId, blocks)
					}
				}
			}

		}
	}
}
