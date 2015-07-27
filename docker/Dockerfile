FROM ubuntu:14.04
MAINTAINER jaybooth@gmail.com

# single node 
RUN apt-get install -y libleveldb-dev attr fuse curl
ADD mfs /bin/mfs
VOLUME /var/mfs
#RUN mfs singlenode 3 1 2  /var/mfs/data /var/mfs/mnt &> ~/mfs.out &
CMD /bin/bash
