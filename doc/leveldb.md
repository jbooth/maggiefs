Your package manager of choice might be a little bit behind for the version of leveldb we're using.  Specifically, Ubuntu prior to 13.04 is behind.  To install manually:

Download leveldb version 1.9 or higher:

wget https://leveldb.googlecode.com/files/leveldb-1.9.0.tar.gz  
tar xzvf leveldb-1.9.0.tar.gz  
cd leveldb-1.9.0 && make   

This directory is your LEVELDB_HOME.

Then add the following lines to your .bashrc so go can use them to link leveldb code

export CGO_CFLAGS="-I/$LEVELDB_HOME"  
export CGO_LDFLAGS="-L/$LEVELDB_HOME"  
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LEVELDB_HOME  

Now run 

go get github.com/jbooth/maggiefs/mfs 
go install github.com/jbooth/maggiefs/mfs 

and you'll have the maggiefs binary installed
