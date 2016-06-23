Usage: java -Dshosts=host1:port1,... -Ddhost=host2:port2 -Dtype=... -Dtime=<seconds> -Dmatch='...' -cp <jarname> clustercopier.ClusterCopier <br>
Where:<br>
  `shosts` is the source cluster or a list of non-cluster redis instances.<br>
    If the first element of the list is a cluster element then the copy will treat the list as references to a single cluster.<br>
    If the first element of the list is a non-cluster instance then the list will be treated as a series of instances to copy from.<br>
    Only one (master) instance from a cluster is needed.<br>
  `dhost` is the destination cluster or redis instance.  Only one instance may be specified.<br>
  `type` is the optional redis data type of the data to be copy.  Records not of this type are skipped.<br>
  `time` is the optional length of time to run in seconds.<br>
    Usefull for development purposes.  Default is as long as it takes.<br>
  `match` is the redis pattern to use in the scan to copy.  Defaults to all keys<br>

----------------------

This is an `Eclipse/Ivy` project.  Its purpose is to copy all the keys
from a `redis cluster`, or one _or more_ `redis instances` to a **single**
destination cluster or instance.

By _default_ all keys are copied _including_ each key's associated `TTL`.
The `match` parameter allows you to specify a pattern for keys to copy
as opposed to all keys.  In addition you can specify a `type` to
narrow down the copy to a particular redis data type.

The copier runs for as long as it takes and prints out status every so
often.  At the end it prints totals.  For test runs a `time` parameter
can be specified to limit how long the copier runs.

The `shosts` parameter is specified as one or more _comma_ separated
`host:port` specifications.  _If_ the first element of the list is a
_cluster_ then it is *assumed* that all the redis instances in the list
are members of the **same** cluster.  _If_ the first is a _non-clustered_
instance then the list is interpreted as _multiple_ sources from which
keys are copied to the destination.  The destination is **only a single instance** .
It is not possible to copy to multiple destinations.
--------------------

**Building**

To build in `Eclipse` click on the `ClusterCopier` java file and right click.
Select `Export`/ as `Runnable Jar`.  Fill in the appropriate output names
and main class.  I generally find that if you run the ClusterCopier
once in Eclipse that it is easier to select the context to export
from.

The usage instructions show a _slightly_ different way to run the
program.  If you run the jar file exported from Eclipse, it is only
necessary to use `-jar jarname.jar` rather than specifying the jar
file name in the class path and the main class name.
