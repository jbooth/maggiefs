package conf

import (
  "fmt"
  "os"
)




func NewConfSet(volRoots [][]string, nameHome string, bindHost string, startPort int, replicationFactor uint32, format bool) (*NSConfig, []*DSConfig) {
  nncfg := &NSConfig{}
  nncfg.LeaseBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
  startPort++
  nncfg.NameBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
  startPort++
  nncfg.WebBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
  startPort++
  nncfg.NNHomeDir = nameHome
  nncfg.ReplicationFactor = replicationFactor
  dscfg := make([]*DSConfig, len(volRoots))
  for idx, dnVolRoots := range volRoots {
    thisDscfg := &DSConfig{}
    thisDscfg.DataClientBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
    startPort++
    thisDscfg.NameDataBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
    startPort++
    thisDscfg.WebBindAddr = fmt.Sprintf("%s:%d", bindHost, startPort)
    startPort++
    thisDscfg.VolumeRoots = dnVolRoots
    thisDscfg.LeaseAddr = nncfg.LeaseBindAddr
    thisDscfg.NameAddr = nncfg.NameBindAddr
    dscfg[idx] = thisDscfg

  }
  return nncfg, dscfg
}

func NewConfSet2(numDNs int, volsPerDn int, replicationFactor uint32, baseDir string) (*NSConfig, []*DSConfig, error) {
  err := os.Mkdir(baseDir, 0777)
  if err != nil {
    return nil, nil, err
  }
  nameBase := baseDir + "/name"
  err = os.Mkdir(nameBase, 0777)
  if err != nil {
    return nil, nil, err
  }

  dataBase := baseDir + "/data"
  err = os.Mkdir(dataBase, 0777)
  if err != nil {
    return nil, nil, err
  }
  volRoots := make([][]string, numDNs)
  for i := 0; i < numDNs; i++ {
    dnBase := fmt.Sprintf("%s/dn%d", dataBase, i)
    err = os.Mkdir(dnBase, 0777)
    if err != nil {
      return nil, nil, fmt.Errorf("TestCluster: Error trying to create dn base dir %s : %s\n", dnBase, err.Error())
    }
    dnRoots := make([]string, volsPerDn)
    for j := 0; j < volsPerDn; j++ {
      dnRoots[j] = fmt.Sprintf("%s/vol%d", dnBase, j)
      err = os.Mkdir(dnRoots[j], 0777)
      if err != nil {
        return nil, nil, fmt.Errorf("TestCluster: Error trying to create dir %s : %s\n", dnRoots[j], err.Error())
      }
    }
    volRoots[i] = dnRoots
  }
  nnc, dsc := NewConfSet(volRoots, nameBase, "0.0.0.0", 11001, replicationFactor, true)
  return nnc, dsc, nil
}