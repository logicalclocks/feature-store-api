# Managed RonDB
For applications where Feature Store's performance and scalability is paramount we give users the option to create clusters with
Managed [RonDB](https://www.rondb.com/). You don't need to worry about configuration as [hopsworks.ai](https://managed.hopsworks.ai/) will
automatically pick the best options for your setup.

## Enabling RonDB
To setup a cluster with RonDB, click on the `Enable` checkbox during cluster creation. If you don't see this option contact [us](mailto: sales@logicalclocks.com).

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/rondb/rondb_enable.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/rondb/rondb_enable.png" alt="Enable Managed RonDB">
    </a>
    <figcaption>Enable Managed RonDB</figcaption>
  </figure>
</p>

## Node selection
If you enable Managed RonDB you will see a basic configuration page where you can configure the database nodes.

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/rondb/rondb_basic.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/rondb/rondb_basic.png" alt="RonDB basic configuration">
    </a>
    <figcaption>RonDB basic configuration</figcaption>
  </figure>
</p>

First, you need to select the number of *Data nodes*. These are the database nodes that will store data. The number
of Datanodes is dependent on the [number of replicas](#number-of-replicas). For most applications 2 Datanodes will suffice with
memory optimized instance type. Local storage is not very important as RonDB is an in-memory database but it has to be
big enough for offline storage of recovery data.

Next you can configure the number of MySQLd nodes. If you have a specific use-case where you expect high load on MySQL servers,
such as a custom streaming application performing SQL queries in short interval, then you can add more. In a general use-case
you can go with zero, cluster's Head node already comes with a MySQL server.

## Advanced
In this section you can change advanced settings of RonDB. Proceed **only** if you know what you are doing.

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/rondb/rondb_advanced.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/rondb/rondb_advanced.png" alt="RonDB advanced configuration">
    </a>
    <figcaption>RonDB advanced configuration</figcaption>
  </figure>
</p>

### Number of replicas
RonDB is a distributed in-memory database. To provide high-availability RonDB replicates data on different node groups. The
default number of replicas is 2 which covers most of the cases. If you change this value, you must change the number of
*Datanodes* accordingly. The number of Datanodes configured in the `Basic` tab should be evenly divisible by the numer of replicas.

### API nodes
API nodes are specialized nodes which can run user code connecting directly to RonDB datanodes for increased performance.

One use-case which might be interesting is to *benchmark* RonDB. In that case you will need at least one API node and you must
select the checkbox which **grants access** to a benchmark user to specific tables in MySQL.

## RonDB details
Once the cluster is created you can view some details by clicking on the `RonDB` tab as shown in the picture below.

<p align="center">
  <figure>
    <a href="../../assets/images/hopsworksai/rondb/rondb_details.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/rondb/rondb_details.png" alt="RonDB cluster details">
    </a>
    <figcaption>RonDB cluster details</figcaption>
  </figure>
</p>
