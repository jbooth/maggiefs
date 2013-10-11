maggiefs
========

A fully posix-compliant distributed filesystem 

Why Maggiefs?
==
Why a new distributed file system?  In short, because the available DFSes are limited either by their feature-set or their architecture.  The two most successful open-source distributed filesystems are Hadoop and GlusterFS.  

Hadoop is a properly distributed filesystem with facility for dispatching computation to the node it's located and saving network load.  However, it lacks a POSIX API or any sort of unix integration.  It's a java library which takes strings that look a lot like paths, but is not usable by any non-hadoop application.  Users and permissions must be configured separately from your basic unix users, and all interaction with the filesystem has to be done via API or via Hadoop's cumbersome CLI tool.  Additionally, files are write-once, append-only (no random writes), and its I/O architecture is optimized for long streaming reads in a way which severely disadvantages random reads.  MaggieFS borrows several ideas from the Hadoop architecture but is focused on a POSIX API, natively readable by any application on your server, and emphasizes random read speed (necessarily since kernels make sequential random reads for file streaming).  OS read-ahead compensates for latency problems with stateless reads.

GlusterFS is more of a SAN than it is a properly distributed filesystem.  It exposes volumes as block devices, and then it's up to the administrator to configure "translators" in front of these block devices which stripe and mirror data, which also serve as bottlenecks and points of failure in the system while giving administrators plenty of places to make mistakes.    This means that your application is very rarely reading local data -- data must be funneled through one or many translators, separating your cluster into compute and storage nodes, rather than a single grid.  MaggieFS by comparison has a single (soon to be multiple) master(s) and everything else is a peer.  As a peer, you export your drives, you get access to the filesystem, and you don't have to worry about configuring your networked storage topology.  

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
======

The mfs binary has 4 operation modes (and a couple utilities).

    mfs singlenode [numDatanodes] [volumesPerDN] [replicationFactor] [baseDir for data] [mountPoint] 

mfs singlenode runs a mock cluster by building out directories under a temp directory.  It's useful for testing or test-driving.  If you wanted to run a mock cluster with 3 DNs, 1 volume each and a replicationFactor of 2, you could run:

    mfs singlenode 3 1 2 /tmp/maggiefsData /tmp/maggiefsMount
    
Runs nameserver

    mfs dataserver [configPath] [localMountPoint] ######
    
For dataservers, we tend to run a client and mount somewhere on that machine as part of the same process, to facilitate certain optimizations for local data.

    mfs client [nameHost] [leaseHost] [localMountPoint] ######
    
To run standalone client to existing cluster
