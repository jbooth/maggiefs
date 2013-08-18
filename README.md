maggiefs -- A fully posix-compliant distributed filesystem 
====

Design
==
Architecturally, MaggieFS is very similar to the Hadoop Distributed Filesystem (HDFS) with a model of one namenode and N datanodes.  It adds a third component, the leaseserver, which is co-located with the nameserver and responsible for establishing happens-before relationships so that we can guarantee consistency across the cluster when files are changed.  

The filesystem implements client functions using the Fuse low-level API, which provides a tree model to the kernel and the kernel walks that tree to resolve paths, reading/writing data from files by inode id.  In terms of performance, the kernel routines for managing readahead and VFS cache should benefit MaggieFS and likely make it more performant than Hadoop for random reads while equivalent for streaming reads, once MaggieFS itself is free of glaring performance holes.

The goal of this filesystem is to provide posix semantics and full read-write capability, to be faster than HDFS in some situations, and to take up fewer system resources.

To Install
==

The mfs binary is responsible for running the nameserver and dataserver processes, as well as the client.

To install, first set up your $GOPATH according to [standard go project conventions](http://golang.org/doc/code.html).

mfs has a dependency on the leveldb library, version 1.9.0.  If you're using ubuntu 13.04, it should be available using:

apt-get libleveldb-dev

If your package manager doesn't have a recent enough version of levelDB, follow the [mfs levelDB installation instructions](doc/leveldb.md) to set up go to build against a downloaded version of the library.

Finally, run:

go get github.com/jbooth/maggiefs/mfs  
go install github.com/jbooth/maggiefs/mfs 

And you'll have the mfs binary in $GOPATH/bin.

To Run
==

The mfs binary has 4 operation modes (and a couple utilities).

###### mfs singlenode ######
mfs singlenode runs a mock cluster by building out directories under a temp directory.  It's primarily used for testing.  




###### mfs dataserver ######
For dataservers, we tend to run a client and mount somewhere on that machine as part of the same process, to facilitate certain optimizations for local data.

go get 

