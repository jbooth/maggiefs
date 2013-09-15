#!/bin/bash

# Set up a cluster, takes a file with a list of hosts as an argument.
# We take the first host as the master, the rest as the peers,
# wire everybody with their configs and then launch their services.

# Assumes a properly installed go environment with the leveldb library 1.9 on the LD_LIBRARY_PATH

if [ $# -ne 1 ]
then
  echo "Usage: setup-cluster.sh <hostsFile.txt>"
  exit 1
fi

#re-editable params
masterhome="/var/mfsMaster"
datahome="/var/mfsData"
mountPoint="/mfs"


masterhost=`head -1 $1`
echo "installing master to ${masterhost}"
ssh ${masterhost} /bin/bash --login << EOFMASTER
./.bash_profile
go clean -i -r github.com/jbooth/maggiefs/mfs
go get github.com/jbooth/maggiefs/mfs
go install github.com/jbooth/maggiefs/mfs
rm -rf ${masterhome}/data
mkdir -p ${masterhome}/data
mfs format ${masterhome}/data
mfs masterconfig ${masterhome}/data
mfs masterconfig ${masterhome}/data > ~/master.cfg
nohup mfs master ~/master.cfg &> ~/master.log &
EOFMASTER

sleep 2 

for peerhost in $(tail -n +2 $1)
do 
echo "installing peer to ${peerhost}"
ssh ${peerhost} /bin/bash --login << EOFPEER
    ./.bash_profile
    go clean -i -r github.com/jbooth/maggiefs/mfs
    go get github.com/jbooth/maggiefs/mfs
    go install github.com/jbooth/maggiefs/mfs
    echo "mfs peerconfig -masterAddr ${masterhost} -bindAddr ${peerhost} -mountPoint ${mountPoint} -volRoots ${datahome} "
    mfs peerconfig -masterAddr ${masterhost} -bindAddr ${peerhost} -mountPoint "${mountPoint}" -volRoots "${datahome}" 
    mfs peerconfig -masterAddr ${masterhost} -bindAddr ${peerhost} -mountPoint "${mountPoint}" -volRoots "${datahome}" > ~/peer.cfg
    sudo chmod a+r /etc/fuse.conf
    nohup mfs peer ~/peer.cfg &>~/peer.log &
EOFPEER
done
