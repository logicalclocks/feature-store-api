# Spark Integration

Connecting to the Feature Store from an external Spark cluster, such as Cloudera, requires configuring it with the Hopsworks client jars and configuration. This guide explains step by step how to connect to the Feature Store from an external Spark cluster.

## Download the Hopsworks Client Jars

In the Feature Store UI, select the *integration* tab and then select the *Spark* tab. Click on *Download client Jars*. This will start the download of the *client.tar.gz* archive. The archive contains two jar files for HopsFS, the client libraries for HopsHive and the Java version of the HSFS library. You should upload these libraries to your Spark cluster and attach them as local resources to your Job. If you are using `spark-submit`, you should specify the `--jar` option. For more details see: [Spark Dependency Management](https://spark.apache.org/docs/latest/submitting-applications.html#advanced-dependency-management).

<p align="center">
    <figure>
        <img src="../../assets/images/spark_integration.png" alt="Spark integration tab">
        <figcaption>The Spark Integration gives access to Jars and configuration for an external Spark cluster</figcaption>
    </figure>
</p>

## Download the certificates

Download the certificates from the same *Spark* tab in the Feature Store UI. Hopsworks uses X.509 certificates for authentication and authorization. If you are interested in the Hopsworks security model, you can read more about it in this [blog post](https://www.logicalclocks.com/blog/how-we-secure-your-data-with-hopsworks).
The certificates are composed of three different components: the `keyStore.jks` containing the private key and the certificate for your project user, the `trustStore.jks` containing the certificates for the Hopsworks certificates authority, and a password to unlock the private key in the `keyStore.jks`. The password is displayed in a pop-up when downloading the certificate and should be saved in a file named `material_passwd`.

!!! warning
    When you copy-paste the password to the `material_passwd` file, pay attention to not introduce additional empty spaces or new lines.

The three files (`keyStore.jks`, `trustStore.jks` and `material_passwd`) should be attached as resources to your Spark application as well.

## Configure your Spark cluster

Add the following configuration to the Spark application:

```
spark.hadoop.fs.hopsfs.impl                         io.hops.hopsfs.client.HopsFileSystem
spark.hadoop.hops.ipc.server.ssl.enabled            true
spark.hadoop.hops.ssl.hostname.verifier             ALLOW_ALL
spark.hadoop.hops.rpc.socket.factory.class.default  io.hops.hadoop.shaded.org.apache.hadoop.net.HopsSSLSocketFactory
spark.hadoop.client.rpc.ssl.enabled.protocol        TLSv1.2
spark.hadoop.hops.ssl.keystores.passwd.name         material_passwd
spark.hadoop.hops.ssl.keystore.name                 keyStore.jks
spark.hadoop.hops.ssl.trustore.name                 trustStore.jks

spark.sql.hive.metastore.jars                       [Path to the Hopsworks Hive Jars]
spark.hadoop.hive.metastore.uris                    thrift://[metastore_ip]:[metastore_port]
```

`spark.sql.hive.metastore.jars` should point to the path with the Hive Jars which can be found in the *clients.tar.gz*.

## PySpark

To use PySpark, install the HSFS Python library which can be found on [PyPi](https://pypi.org/project/hsfs/).

!!! attention "Matching Hopsworks version"
    The **major version of `HSFS`** needs to match the **major version of Hopsworks**.


<p align="center">
    <figure>
        <img src="../../assets/images/hopsworks-version.png" alt="HSFS version needs to match the major version of Hopsworks">
        <figcaption>You find the Hopsworks version inside any of your Project's settings tab on Hopsworks</figcaption>
    </figure>
</p>

## Generating an API Key

In Hopsworks, click on your *username* in the top-right corner and select *Settings* to open the user settings. Select *Api keys*. Give the key a name and select the job, featurestore and project scopes before creating the key. Copy the key into your clipboard for the next step.

!!! success "Scopes"
    The created API-Key should at least have the following scopes:

    1. featurestore
    2. project
    3. job

<p align="center">
  <figure>
    <img src="../../assets/images/api-key.png" alt="Generating an API Key on Hopsworks">
    <figcaption>API-Keys can be generated in the User Settings on Hopsworks</figcaption>
  </figure>
</p>

!!! info
    You are only ably to retrieve the API Key once. If you did not manage to copy it to your clipboard, delete it again and create a new one.

## Connecting to the Feature Store

You are now ready to connect to the Hopsworks Feature Store from Spark:

```python
import hsfs
conn = hsfs.connection(
    host='my_instance',                 # DNS of your Feature Store instance
    port=443,                           # Port to reach your Hopsworks instance, defaults to 443
    project='my_project',               # Name of your Hopsworks Feature Store project
    api_key_value='api_key',            # The API key to authenticate with the feature store
    hostname_verification=True          # Disable for self-signed certificates
)
fs = conn.get_feature_store()           # Get the project's default feature store
```

!!! note "Engine"

    `HSFS` uses either Apache Spark or Apache Hive as an execution engine to perform queries against the feature store. The `engine` option of the connection let's you overwrite the default behaviour by setting it to `"hive"` or `"spark"`. By default, `HSFS` will try to use Spark as engine if PySpark is available, hence, no further action should be required if you setup Spark correctly as described above.

## Next Steps

For more information about how to connect, see the [Connection](../generated/project.md) guide. Or continue with the Data Source guide to import your own data to the Feature Store.
