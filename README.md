Usage: java -Dshosts=host1:port1,... -Ddhost=host2:port2 -Dtype=... -Dtime=<seconds> -Dmatch='...' -cp <jarname> clustercopier.ClusterCopier 
Where:
  shosts is the source cluster or a list of non-cluster redis instances.
    If the first element of the list is a cluster element then the copy will treat the list as references to a single cluster.
    If the first element of the list is a non-cluster instance then the list will be treated as a series of instances to copy from.
    Only one (master) instance from a cluster is needed.
  dhost is the destination cluster or redis instance.  Only one instance may be specified.
  type is the optional redis data type of the data to be copy.  Records not of this type are skipped.
  time is the optional length of time to run in seconds.  Usefull for development purpose.  Default is as long as it takes.
  match is the redis pattern to use in the scan to copy.  Defaults to all keys

----------------------

This is an Eclipse/Ivy project.  Its purpose is to copy all the keys
from a redis cluster, or one or more redis instances to a single
destination cluster or instance.

By default all keys are copied including each key's associated TTL.
The 'match' parameter allows you to specify a pattern for keys to copy
as opposed to all keys.  In addition you can specify a 'type' to
narrow down the copy to a particular redis data type.

The copier runs for as long as it takes and prints out status every so
often.  At the end it prints totals.  For test runs a 'time' parameter
can be specified to limit how long the copier runs.

The 'shosts' parameter is specified as one or more comma separated
host:port specifications.  If the first element of the list is a
cluster then it is assumed that all the redis instances in the list
are members of the same cluster.  If the first is a non-clustered
instance then the list is interpreted as multiple sources from which
keys are copied to the destination.  The destination is only a single
instance.  It is not possible to copy to multiple destinations.

Building

To build in Eclipse click on the ClusterCopier java file and right click.
Select Export/ as Runnable Jar.  Fill in the appropriate output names
and main class.  I generally find that if you run the ClusterCopier
once in Eclipse that it is easier to select the context to export
from.

The usage instructions show a slightly different way to run the
program.  If you run the jar file exported from Eclipse, it is only
necessary to use '-jar jarname.jar' rather than specifying the jar
file name in the class path and the main class name.
