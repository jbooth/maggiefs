usage: mfs [-debug] [-cpuprofile <filePath>] [-blockprofile <filePath>] <cmd>

Optional arguments
  -blockprofile="": file to write block profiling information to
  -cpuprofile="": file to write CPU profiling information to
  -debug=false: print debug info about which fuse operations we're doing and their errors

Command arguments:
  mfs master <path/to/config> : run a master
  mfs peer <path/to/config> : run a peer
  mfs masterconfig <homedir> : prints defaulted master config to stdout, with homedir set as the master's home
  mfs peerconfig [options] : prints peer config to stdout, run mfs peerconfig -usage to see options 
  mfs format <path/to/NameDataDir>: formats a directory for use as the master's homedir 
  mfs singlenode <numDNs> <volsPerDn> <replicationFactor> <baseDir> <mountPoint>

