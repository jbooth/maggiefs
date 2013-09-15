#!/bin/bash

# Set up a cluster, takes a file with a list of hosts as an argument.
# We take the first host as the master, the rest as the peers,
# wire everybody with their configs and then launch their services.

#re-editable params
masterhome="/var/mfsMaster"
datahome="/var/mfsData"
masterhost=`head -1 $1`
echo "masterhost ${masterhost}"

ssh ${masterhost} << EOFMASTER
  go clean -i github.com/jbooth/maggiefs/mfs
  go install github.com/jbooth/maggiefs/mfs
  rm -rf ${masterhome}/data
  mkdir -p ${masterhome}/data
  mfs format ${masterhome}/data
  mfs masterconfig ~/masterHome > ~/master.cfg
  nohup mfs master ~/master.cfg &
EOFMASTER

#why not
sleep 5 

for peerhost in $(tail -n +2 $1)
  do
    ssh ${peerhost} << EOFPEER
    go clean -i github.com/jbooth/maggiefs/mfs
    go clean -i github.com/jbooth/maggiefs/mfs
    mfs peerconfig -masterHost ${nameHost} -
    EOFPEER
    echo "peer ${peerhost}"
done
