To interoperate with Hadoop, you need to have a maggieFS mountpoint on all tasktrackers and machines you plan to use the hadoop fs command line utility from.

Once you have that, you must compile our hadoop interoperability jar, install it on all nodes, and configure it to use the local mountpoint.

From the project root:

cd hadoop 
mvn package 

Now, you need to copy target/mfs-hadoop-x-x-x.jar to the lib/ folder of your hadoop installation on all machines in your cluster.

Having done that, set the following values in conf/core-site.xml in the hadoop installation for each machine:
<property> 
  <name>fs.default.name</name> 
  <value>mfs://localhost:1103/tmp/maggiefs</value> 
</property> 
<property>
  <name>fs.mfs.impl</name> 
  <value>org.maggiefs.hadoop.MaggieFileSystem</value> 
</property>

Finally, restart any job trackers and task trackers.  The hadoop fs command and all mapreduce tasks will now use your MFS mounts on each machine.
