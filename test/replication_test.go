package test

import (
	"fmt"
	"github.com/jbooth/maggiefs/maggiefs"
	"os"
	"testing"
	"net/http"
	"encoding/json"
)

func TestAddInodeToCluster(t *testing.T) {
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		panic(err)
	}
	ino.Inodeid = id
	fmt.Println("getting node from cluster")
	ino2, err := testCluster.Names.GetInode(id)
	if !ino.Equals(ino2) {
		t.Fatal(fmt.Errorf("Error, inodes not equal : %+v : %+v\n", *ino, *ino2))
	}

}

func TestAddBlock(t *testing.T) {
	fmt.Println("Adding node to cluster")
	ino := maggiefs.NewInode(0, maggiefs.FTYPE_REG, 0755, uint32(os.Getuid()), uint32(os.Getgid()))
	id, err := testCluster.Names.AddInode(ino)
	if err != nil {
		t.Fatal(err)
	}
	ino.Inodeid = id
	newBlock, err := testCluster.Names.AddBlock(ino.Inodeid, 1024)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("got block back %+v\n", newBlock)
	ino, err = testCluster.Names.GetInode(ino.Inodeid)

	if newBlock.Id != ino.Blocks[0].Id || ino.Blocks[0].EndPos != 1023 {
		// 1023 end pos for 1024 length because we're 0 indexed
		t.Fatal(fmt.Errorf("Wrong end length for block %+v", ino.Blocks[0]))
	}
	// check that block made it to each datanode
	fstat, err := testCluster.Names.StatFs()
	fmt.Printf("got fstat %+v\n", fstat)
	for _, dnInfo := range fstat.DnStat {
		for _, volStat := range dnInfo.Volumes {
			volId := volStat.VolId
			for _, blockVolId := range newBlock.Volumes {
				if blockVolId == volId {
					fmt.Printf("looking for vol %d on dn %d\n", volId, dnInfo.DnId)
					// dnIDs start at 1 so decrement
					blocks, err := testCluster.DataNodes[dnInfo.DnId-1].BlockReport(volId)
					if err != nil {
						t.Fatal(err.Error())
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
		// check that each datanode can pull inode json
		inoJsonAddr := fmt.Sprintf("http://%s/inode?inodeid=%d", testCluster.DataNodes[dnInfo.DnId - 1].HttpAddr(), ino.Inodeid)
		fmt.Printf("Getting ino json from %s\n", inoJsonAddr)
		response, err := http.Get(inoJsonAddr)
		if err != nil {
			panic(err)
		}
		ino3 := &maggiefs.Inode{}
		respBytes := make([]byte, response.ContentLength)
		response.Body.Read(respBytes)
		fmt.Printf("Got json %s\n", string(respBytes))
		json.Unmarshal(respBytes, ino3)
		if !ino.Equals(ino3) {
			t.Fatal(fmt.Errorf("Error, inode from JSON not equal : %+v : %+v\n", *ino, *ino3))
		}
	}

}
