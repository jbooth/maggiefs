maggiefs -- A fully posix-compliant distributed filesystem 
====

Design
====
Architecturally, MaggieFS is very similar to the Hadoop Distributed Filesystem (HDFS) with a model of one namenode and N datanodes.  It adds a third component, the leaseserver, which is co-located with the nameserver and responsible for establishing happens-before relationships so that we can guarantee consistency across the cluster when files are changed.  

The filesystem implements client functions using the Fuse low-level API, which provides a tree model to the kernel and the kernel walks that tree to resolve paths, reading/writing data from files by inode id.  In terms of performance, the kernel routines for managing readahead and VFS cache should benefit MaggieFS and likely make it more performant than Hadoop for random reads while equivalent for streaming reads, once MaggieFS itself is free of glaring performance holes.

The goal of this filesystem is to provide posix semantics and full read-write capability, to be faster than HDFS in some situations, and to take up fewer system resources.

To Run
====

The mfs binary is responsible for running the nameserver and dataserver processes, as well as a client 
go get 

