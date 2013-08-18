#!/bin/bash

# you should set LEVELDB_HOME and then add the following lines to your .bashrc
#LEVELDB_HOME=/home/jay/leveldb-1.9.0

export CGO_CFLAGS="-${LEVELDB_HOME}/include" 
export CGO_LDFLAGS="-L${LEVELDB_HOME}"
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:${LEVELDB_HOME}
