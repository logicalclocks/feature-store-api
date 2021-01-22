# Configure EMR for the Hopsworks Feature Store
To enable EMR to access the Hopsworks Feature Store, you need to set up a Hopsworks API key, add a bootstrap action and configurations to your EMR cluster.

!!! info
    Ensure [Networking](networking.md) is set up correctly before proceeding with this guide.

## Step 1: Set up a Hopsworks API key
In order for EMR clusters to be able to communicate with the Hopsworks Feature Store, the clients running on EMR need to be able to access a Hopsworks API key.

### Generate an API key

In Hopsworks, click on your *username* in the top-right corner and select *Settings* to open the user settings. Select *API keys*. Give the key a name and select the project scope before creating the key. Copy the key into your clipboard for the next step.

!!! success "Scopes"
    The API key should contain at least the following scopes:

    1. project

<p align="center">
  <figure>
    <img src="../../../assets/images/emr/api_key.png" alt="Generating an API key on Hopsworks">
    <figcaption>API keys can be created in the User Settings on Hopsworks</figcaption>
  </figure>
</p>

!!! info
    You are only able to retrieve the API key once. If you did not manage to copy it to your clipboard, delete it and create a new one.

### Store the API key in the AWS Secrets Manager

In the AWS management console ensure that your active region is the region you use for EMR.
Go to the *AWS Secrets Manager* and select *Store new secret*. Select *Other type of secrets* and add *api-key*
as the key and paste the API key created in the previous step as the value. Click next.

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/aws/databricks_secrets_manager_step_1.png">
      <img src="../../../assets/images/databricks/aws/databricks_secrets_manager_step_1.png" alt="Store a Hopsworks API key in the Secrets Manager">
    </a>
    <figcaption>Store a Hopsworks API key in the Secrets Manager</figcaption>
  </figure>
</p>

As a secret name, enter *hopsworks/featurestore*. Select next twice and finally store the secret.
Then click on the secret in the secrets list and take note of the *Secret ARN*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/emr/secrets_manager.png">
      <img src="../../../assets/images/emr/secrets_manager.png" alt="Name the secret">
    </a>
    <figcaption>Name the secret</figcaption>
  </figure>
</p>

### Grant access to the secret to the EMR EC2 instance profile

Identify your EMR EC2 instance profile in the EMR cluster summary:
<p align="center">
  <figure>
    <a  href="../../../assets/images/emr/emr_instance_profile.png">
      <img src="../../../assets/images/emr/emr_instance_profile.png" alt="Identify your EMR EC2 instance profile">
    </a>
    <figcaption>Identify your EMR EC2 instance profile</figcaption>
  </figure>
</p>


In the AWS Management Console, go to *IAM*, select *Roles* and then the EC2 instance profile used by your EMR cluster.
Select *Add inline policy*. Choose *Secrets Manager* as a service, expand the *Read* access level and check *GetSecretValue*.
Expand Resources and select *Add ARN*. Paste the ARN of the secret created in the previous step.
Click on *Review*, give the policy a name und click on *Create policy*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/emr/emr_policy.png">
      <img src="../../../assets/images/emr/emr_policy.png" alt="Configure the access policy for the Secrets Manager">
    </a>
    <figcaption>Configure the access policy for the Secrets Manager</figcaption>
  </figure>
</p>

## Step 2: Configure your EMR cluster

### Add the Hopsworks Feature Store configuration to your EMR cluster
In order for EMR to be able to talk to the Feature Store, you need to update the Hadoop and Spark configurations.
Copy the configuration below and replace ip-XXX-XX-XX-XXX.XX-XXXX-X.compute.internal with the private DNS name of your Hopsworks master node.

```json
[
  {
    "Classification": "hadoop-env",
    "Properties": {

    },
    "Configurations": [
      {
        "Classification": "export",
        "Properties": {
          "HADOOP_CLASSPATH": "$HADOOP_CLASSPATH:/usr/lib/hopsworks/client/*"
        },
        "Configurations": [

        ]
      }
    ]
  },
  {
    "Classification": "core-site",
    "Properties": {
      "fs.hopsfs.impl": "io.hops.hopsfs.client.HopsFileSystem",
      "hops.ipc.server.ssl.enabled": true,
      "hops.ssl.hostname.verifier": "ALLOW_ALL",
      "hops.rpc.socket.factory.class.default": "io.hops.hadoop.shaded.org.apache.hadoop.net.HopsSSLSocketFactory",
      "client.rpc.ssl.enabled.protocol": "TLSv1.2",
      "hops.ssl.keystores.passwd.name": "/usr/lib/hopsworks/material_passwd",
      "hops.ssl.keystore.name": "/usr/lib/hopsworks/keyStore.jks",
      "hops.ssl.trustore.name": "/usr/lib/hopsworks/trustStore.jks"
    }
  },
  {
    "Classification": "spark-defaults",
    "Properties": {
      "spark.sql.hive.metastore.jars": "/usr/lib/hopsworks/apache-hive-bin/lib/*",
      "spark.executor.extraClassPath": "/usr/lib/hopsworks/client/*",
      "spark.driver.extraClassPath": "/usr/lib/hopsworks/client/*"
    }
  },
  {
    "Classification": "spark-hive-site",
    "Properties": {
      "hive.metastore.uris": "thrift://ip-XXX-XX-XX-XXX.XX-XXXX-X.compute.internal:9083"
    }
  }
]
```

When you create your EMR cluster, add the configuration:

!!! note
    Don't forget to replace ip-XXX-XX-XX-XXX.XX-XXXX-X.compute.internal with the private DNS name of your Hopsworks master node.

<p align="center">
  <figure>
    <a  href="../../../assets/images/emr/emr_config.png">
      <img src="../../../assets/images/emr/emr_config.png" alt="Configure EMR to access the Feature Store">
    </a>
    <figcaption>Configure EMR to access the Feature Store</figcaption>
  </figure>
</p>

### Add the Bootstrap Action to your EMR cluster

EMR requires Hopsworks connectors to be able to communicate with the Hopsworks Feature Store. These connectors can be installed with the
bootstrap action shown below. Copy the content into a file and name the file `hopsworks.sh`. Copy that file into any S3 bucket that
is readable by your EMR clusters and take note of the S3 URI of that file e.g., `s3://my-emr-init/hopsworks.sh`.

```bash
#!/bin/bash
set -e

if [ "$#" -ne 3 ]; then
    echo "Usage hopsworks.sh HOPSWORKS_API_KEY_SECRET, HOPSWORKS_HOST, PROJECT_NAME"
    exit 1
fi

SECRET_NAME=$1
HOST=$2
PROJECT=$3

API_KEY=$(aws secretsmanager get-secret-value --secret-id $SECRET_NAME | jq -r .SecretString | jq -r '.["api-key"]')

PROJECT_ID=$(curl -H "Authorization: ApiKey ${API_KEY}" https://$HOST/hopsworks-api/api/project/getProjectInfo/$PROJECT | jq -r .projectId)

sudo yum -y install python3-devel.x86_64 || true

sudo mkdir /usr/lib/hopsworks
sudo chown hadoop:hadoop /usr/lib/hopsworks
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

chmod -R o-rwx /usr/lib/hopsworks
```

Add the bootstrap actions when configuring your EMR cluster. Provide 3 arguments to the bootstrap action: The name of the API key secret e.g., `hopsworks/featurestore`,
the public DNS name of your Hopsworks cluster, such as `ad005770-33b5-11eb-b5a7-bfabd757769f.cloud.hopsworks.ai`, and the name of your Hopsworks project, e.g. `demo_fs_meb10179`.

<p align="center">
  <figure>
    <a  href="../../../assets/images/emr/emr_bootstrap_action.png">
      <img src="../../../assets/images/emr/emr_bootstrap_action.png" alt="Set the bootstrap action for EMR">
    </a>
    <figcaption>Set the bootstrap action for EMR</figcaption>
  </figure>
</p>

Your EMR cluster will now be able to access your Hopsworks Feature Store.

## Next Steps

If you use Python, then install the [HSFS library](https://pypi.org/project/hsfs/). The Scala version of the library has already been installed to your EMR cluster.
Use the [Connection API](../../../generated/api/connection_api/) to connect to the Hopsworks Feature Store. For more information about how to use the Feature Store, see the [Quickstart Guide](../../quickstart.md).

!!! attention "Matching Hopsworks version"
    The **major version of `HSFS`** needs to match the **major version of Hopsworks**.


<p align="center">
    <figure>
        <img src="../../../assets/images/hopsworks-version.png" alt="HSFS version needs to match the major version of Hopsworks">
        <figcaption>You find the Hopsworks version inside any of your Project's settings tab on Hopsworks</figcaption>
    </figure>
</p>
