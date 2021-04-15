# Configure HDInsight for the Hopsworks Feature Store
To enable HDInsight to access the Hopsworks Feature Store, you need to set up a Hopsworks API key, add a script action and configurations to your HDInsight cluster.

!!! info "Prerequisites"
    A HDInsight cluster with cluster type Spark is required to connect to the Feature Store. You can either use an existing cluster or create a new one.

!!! info "Network Connectivity"

    To be able to connect to the Feature Store, please ensure that your HDInsight cluster and the Hopsworks Feature Store are either in the same [Virtual Network](https://docs.microsoft.com/en-us/azure/virtual-network/virtual-networks-overview) or [Virtual Network Peering](https://docs.microsoft.com/en-us/azure/virtual-network/virtual-network-manage-peering) is set up between the different networks. In addition, ensure that the Network Security Group of your Hopsworks instance is configured to allow incoming traffic from your HDInsight cluster on ports 443, 3306, 8020, 30010, 9083 and 9085 (443,3306,8020,30010,9083,9085). See [Network security groups](https://docs.microsoft.com/en-us/azure/virtual-network/network-security-groups-overview) for more information.

## Step 1: Set up a Hopsworks API key
In order for HDInsight clusters to be able to communicate with the Hopsworks Feature Store, the clients running on HDInsight need a Hopsworks API key.

In Hopsworks, click on your *username* in the top-right corner and select *Settings* to open the user settings. Select *API keys*. Give the key a name and select the project scope before creating the key. Make sure you have the key handy for the next steps.

!!! success "Scopes"
    The API key should contain at least the following scopes:

    1. featurestore
    2. project
    3. job

<p align="center">
  <figure>
    <img src="../../assets/images/azure/hdinsight/step-0.png" alt="Generating an API key on Hopsworks">
    <figcaption>API keys can be created in the User Settings on Hopsworks</figcaption>
  </figure>
</p>

!!! info
    You are only able to retrieve the API key once. If you did not manage to copy it to your clipboard, delete it and create a new one.

## Step 2:  Use a script action to install the Feature Store connector

HDInsight requires Hopsworks connectors to be able to communicate with the Hopsworks Feature Store. These connectors can be installed with the script action shown below. Copy the content into a file, name the file `hopsworks.sh` and replace MY_INSTANCE, MY_PROJECT, MY_VERSION, MY_API_KEY and MY_CONDA_ENV with your values. Copy the `hopsworks.sh` file into any storage that is readable by your HDInsight clusters and take note of the URI of that file e.g., `https://account.blob.core.windows.net/scripts/hopsworks.sh`.

The script action needs to be applied head and worker nodes and can be applied during cluster creation or to an existing cluster. Ensure to persist the script action so that it is run on newly created nodes. For more information about how to use script actions, see [Customize Azure HDInsight clusters by using script actions](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-customize-cluster-linux).

!!! attention "Matching Hopsworks version"
    The **major version of `HSFS`** needs to match the **major version of Hopsworks**. Check [PyPI](https://pypi.org/project/hsfs/#history) for available releases.

    <p align="center">
        <figure>
            <img src="../../assets/images/hopsworks-version.png" alt="HSFS version needs to match the major version of Hopsworks">
            <figcaption>You find the Hopsworks version inside any of your Project's settings tab on Hopsworks</figcaption>
        </figure>
    </p>

Feature Store script action:
```bash
set -e

HOST="MY_INSTANCE.cloud.hopsworks.ai" # DNS of your Feature Store instance
PROJECT="MY_PROJECT"                  # Port to reach your Hopsworks instance, defaults to 443
HSFS_VERSION="MY_VERSION"             # The major version of HSFS needs to match the major version of Hopsworks
API_KEY="MY_API_KEY"                  # The API key to authenticate with Hopsworks
CONDA_ENV="MY_CONDA_ENV"              # py35 is the default for HDI 3.6

apt-get --assume-yes install python3-dev
apt-get --assume-yes install jq

/usr/bin/anaconda/envs/$CONDA_ENV/bin/pip install hsfs==$HSFS_VERSION

PROJECT_ID=$(curl -H "Authorization: ApiKey ${API_KEY}" https://$HOST/hopsworks-api/api/project/getProjectInfo/$PROJECT | jq -r .projectId)

mkdir -p /usr/lib/hopsworks
chown root:hadoop /usr/lib/hopsworks
cd /usr/lib/hopsworks

curl -o client.tar.gz -H "Authorization: ApiKey ${API_KEY}" https://$HOST/hopsworks-api/api/project/$PROJECT_ID/client

tar -xvf client.tar.gz
tar -xzf client/apache-hive-*-bin.tar.gz
mv apache-hive-*-bin apache-hive-bin
rm client.tar.gz
rm client/apache-hive-*-bin.tar.gz

curl -H "Authorization: ApiKey ${API_KEY}" https://$HOST/hopsworks-api/api/project/$PROJECT_ID/credentials | jq -r .kStore | base64 -d > keyStore.jks

curl -H "Authorization: ApiKey ${API_KEY}" https://$HOST/hopsworks-api/api/project/$PROJECT_ID/credentials | jq -r .tStore | base64 -d > trustStore.jks

echo -n $(curl -H "Authorization: ApiKey ${API_KEY}" https://$HOST/hopsworks-api/api/project/$PROJECT_ID/credentials | jq -r .password) > material_passwd

chown -R root:hadoop /usr/lib/hopsworks
```

## Step 3: Configure HDInsight for Feature Store access

The Hadoop and Spark installations of the HDInsight cluster need to be configured in order to access the Feature Store. This can be achieved either by using a [bootstrap script](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-customize-cluster-bootstrap) when creating clusters or using [Ambari](https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-manage-ambari) on existing clusters. Apply the following configurations to your HDInsight cluster.

!!! attention "Using Hive and the Feature Store"

    HDInsight clusters cannot use their local Hive when being configured for the Feature Store as the Feature Store relies on custom Hive binaries and its own Metastore which will overwrite the local one. If you rely on Hive for feature engineering then it is advised to write your data to an external data storage such as ADLS from your main HDInsight cluster and in the Feature Store, create an [on-demand](https://docs.hopsworks.ai/overview/#feature-groups) Feature Group on the storage container in ADLS.

Hadoop hadoop-env.sh:
```
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:/usr/lib/hopsworks/client/*
```

Hadoop core-site.xml:
```
hops.ipc.server.ssl.enabled=true
fs.hopsfs.impl=io.hops.hopsfs.client.HopsFileSystem
client.rpc.ssl.enabled.protocol=TLSv1.2
hops.ssl.keystore.name=/usr/lib/hopsworks/keyStore.jks
hops.rpc.socket.factory.class.default=io.hops.hadoop.shaded.org.apache.hadoop.net.HopsSSLSocketFactory
hops.ssl.keystores.passwd.name=/usr/lib/hopsworks/material_passwd
hops.ssl.hostname.verifier=ALLOW_ALL
hops.ssl.trustore.name=/usr/lib/hopsworks/trustStore.jks
```

Spark spark-defaults.conf:
```
spark.executor.extraClassPath=/usr/lib/hopsworks/client/*
spark.driver.extraClassPath=/usr/lib/hopsworks/client/*
spark.sql.hive.metastore.jars=/usr/lib/hopsworks/apache-hive-bin/lib/*
```

Spark hive-site.xml:
```
hive.metastore.uris=thrift://MY_HOPSWORKS_INSTANCE_PRIVATE_IP:9083
```

!!! info
    Replace MY_HOPSWORKS_INSTANCE_PRIVATE_IP with the private IP address of you Hopsworks Feature Store.

## Step 5: Connect to the Feature Store

You are now ready to connect to the Hopsworks Feature Store, for instance using a Jupyter notebook in HDInsight with a PySpark3 kernel:

```python
import hsfs

# Put the API key into Key Vault for any production setup:
# See, https://azure.microsoft.com/en-us/services/key-vault/
secret_value = 'MY_API_KEY'

# Create a connection
conn = hsfs.connection(
    host='MY_INSTANCE.cloud.hopsworks.ai', # DNS of your Feature Store instance
    port=443,                              # Port to reach your Hopsworks instance, defaults to 443
    project='MY_PROJECT',                  # Name of your Hopsworks Feature Store project
    api_key_value=secret_value,            # The API key to authenticate with Hopsworks
    hostname_verification=True             # Disable for self-signed certificates
)

# Get the feature store handle for the project's feature store
fs = conn.get_feature_store()
```

## Next Steps

For more information about how to use the Feature Store, see the [Quickstart Guide](../quickstart.md).
