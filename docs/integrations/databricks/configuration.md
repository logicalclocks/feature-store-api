# Databricks Integration

Users can configure their Databricks clusters to write the results of feature engineering pipelines in the Hopsworks Feature Store using HSFS.
Configuring a Databricks cluster can be done from the Hopsworks Feature Store UI. This guide explains each step.

## Prerequisites

In order to be able to configure a Databricks cluster to use the Feature Store of your Hopsworks instance, it is necessary to ensure networking is setup correctly between the instances and that the Databricks cluster has access to the Hopsworks API key to perform requests with HSFS from Databricks to Hopsworks.

### Networking

If you haven't done so already, follow the networking guides for either [AWS](networking.md#aws) or [Azure](networking.md#azure) for instructions on how to configure networking properly between Databricks' VPC (or Virtual Network on Azure) and the Hopsworks.ai VPC/VNet.

### Hopsworks API Key

In order for the Feature Store API to be able to communicate with the user's Hopsworks instance, the client library (HSFS) needs to have access to a previously generated API Key from Hopsworks. For ways to setup and store the Hopsworks API Key, please refer to the [API Key guide for Databricks](api_key.md).

## Databricks API Key

Hopsworks uses the Databricks REST APIs to communicate with the Databricks instance and configure clusters on behalf of users.
To achieve that, the first step is to register an instance and a valid API Key in Hopsworks.

Users can get a valid Databricks API Key by following the [Databricks Documentation](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token)

!!! warning "Cluster access control"

    If users have enabled [Databricks Cluster access control](https://docs.databricks.com/security/access-control/cluster-acl.html#cluster-access-control), it is important that the users running the cluster configuration (i.e. the user generating the API Key) has `Can Manage` privileges on the cluster they are trying to configure.

## Register a new Databricks Instance

Users can register a new Databricks instance by navigating to the `Integrations` tab of a project Feature Store. Registering a Databricks instance requires adding the instance address and the API Key.

The instance address should be in the format `[UUID].cloud.databricks.com` (or `adb-[UUID].19.azuredatabricks.net` for Databricks on Azure), essentially the same web address used to reach the Databricks instance from the browser.

The API Key will be stored in the Hopsworks secret store for the user and will be available only for that user.  If multiple users need to configure Databricks clusters, each has to generate an API Key and register an instance. The Databricks instance registration does not have a project scope, meaning that once registered, the user can configure clusters for all projects they are part of.

## Databricks Cluster

A cluster needs to exists before users can configure it using the Hopsworks UI. The cluster can be in any state prior to the configuration.

!!! warning "Runtime limitation"

    Currently Runtime 6 is suggested to be able to use the full suite of Hopsworks Feature Store capabilities.

## Configure a cluster

Clusters are configured for a project user, which, in Hopsworks terms, means a user operating within the scope of a project.
To configure a cluster, click on the `Configure` button. By default the cluster will be configured for the user making the request. If the user doesn't have `Can Manage` privilege on the cluster, they can ask a project `Data Owner` to configure it for them. Hopsworks `Data Owners` are allowed to configure clusters for other project users, as long as they have the required Databricks privileges.

During the cluster configuration the following steps will be taken:

- Upload an archive to DBFS containing the necessary Jars for HSFS and HopsFS to be able to read and write from the Hopsworks Feature Store
- Add an initScript to configure the Jars when the cluster is started
- Install `hsfs` python library
- Configure the necessary Spark properties to authenticate and communicate with the Feature Store

When a cluster is configured for a specific project user, all the operations with the Hopsworks Feature Store will be executed as that project user. If another user needs to re-use the same cluster, the cluster can be reconfigured by following the same steps above.

## Connecting to the Feature Store

At the end of the configuration, Hopsworks will start the cluster.
Once the cluster is running users can establish a connection to the Hopsworks Feature Store from Databricks:

!!! note "API Key on Azure"
    Please note, for Azure it is necessary to store the Hopsworks API Key locally on the cluster as a file. As we currently do not support storing the API Key on an Azure Secret Management Service as we do for AWS. Consult the [API Key guide for Azure](api_key.md#azure), for more information.

=== "AWS"

    ```python
    import hsfs
    conn = hsfs.connection(
        'my_instance',                      # DNS of your Feature Store instance
        443,                                # Port to reach your Hopsworks instance, defaults to 443
        'my_project',                       # Name of your Hopsworks Feature Store project
        secrets_store='secretsmanager',     # Either parameterstore or secretsmanager
        hostname_verification=True)         # Disable for self-signed certificates
    )
    fs = conn.get_feature_store()           # Get the project's default feature store
    ```

=== "Azure"

    ```python
    import hsfs
    conn = hsfs.connection(
        'my_instance',                      # DNS of your Feature Store instance
        443,                                # Port to reach your Hopsworks instance, defaults to 443
        'my_project',                       # Name of your Hopsworks Feature Store project
        api_key_file="featurestore.key"     # For Azure, store the API key locally
        hostname_verification=True)         # Disable for self-signed certificates
    )
    fs = conn.get_feature_store()           # Get the project's default feature store
    ```

## Next Steps

For more information about how to connect, see the [Connection](../../generated/project.md) guide. Or continue with the Data Source guide to import your own data to the Feature Store.
