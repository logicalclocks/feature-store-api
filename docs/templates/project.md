# Project/Connection

In Hopsworks [a Project is a sandboxed set](https://www.logicalclocks.com/blog/how-we-secure-your-data-with-hopsworks) of users, data, and programs (where data can be shared in a controlled manner between projects).

Each Project can have its own Feature Store. However, it is possible to [share Feature Stores](#sharing-a-feature-store) among projects.

When working with the Feature Store from a programming [environment](../setup.md) you can connect to a single Hopsworks instance at a time, but it is possible to access multiple Feature Stores simultaneously.

A connection to a Hopsworks instance is represented by a [`Connection` object](#connection). Its main purpose is to retrieve the API Key if you are connecting from an external environment and subsequently to retrieve the needed certificates to communicate with the Feature Store services.

The [handle](#get_feature_store) can then be used to retrieve a reference to the [Feature Store](../generated/feature_store.md) you want to operate on.

## Examples

=== "Python"

    !!! example "Connecting from Hopsworks"
        ```python
        import hsfs
        conn = hsfs.connection()
        fs = conn.get_feature_store()
        ```

    !!! example "Connecting from Databricks"
        In order to connect from Databricks, follow the [integration guide](../integrations/databricks/configuration.md).

        You can then simply connect by using your chosen way of retrieving the API Key:

        ```python
        import hsfs
        conn = hsfs.connection(
            host="ec2-13-53-124-128.eu-north-1.compute.amazonaws.com",
            project="demo_fs_admin000",
            hostname_verification=False,
            secrets_store="secretsmanager"
            )
        fs = conn.get_feature_store()
        ```

        Alternatively you can pass the API Key as a file or directly:

        !!! note "Azure"
            Use this method when working with Hopsworks on Azure.

        ```python
        import hsfs
        conn = hsfs.connection(
            host="ec2-13-53-124-128.eu-north-1.compute.amazonaws.com",
            project="demo_fs_admin000",
            hostname_verification=False,
            api_key_file="featurestore.key"
            )
        fs = conn.get_feature_store()
        ```

    !!! example "Connecting from AWS SageMaker"
        In order to connect from SageMaker, follow the [integration guide](../integrations/sagemaker.md) to setup the API Key.

        You can then simply connect by using your chosen way of retrieving the API Key:

        ```python
        import hsfs
        conn = hsfs.connection(
            host="ec2-13-53-124-128.eu-north-1.compute.amazonaws.com",
            project="demo_fs_admin000",
            hostname_verification=False,
            secrets_store="secretsmanager"
            )
        fs = conn.get_feature_store()
        ```

        Alternatively you can pass the API Key as a file or directly:

        ```python
        import hsfs
        conn = hsfs.connection(
            host="ec2-13-53-124-128.eu-north-1.compute.amazonaws.com",
            project="demo_fs_admin000",
            hostname_verification=False,
            api_key_file="featurestore.key"
            )
        fs = conn.get_feature_store()
        ```

    !!! example "Connecting from Python environment"
        To connect from a simple Python environment, you can provide the API Key as a
        file as shown in the SageMaker example above, or you provide the value directly:

        ```python
        import hsfs
        conn = hsfs.connection(
            host="ec2-13-53-124-128.eu-north-1.compute.amazonaws.com",
            project="demo_fs_admin000",
            hostname_verification=False,
            api_key_value=(
                "PFcy3dZ6wLXYglRd.ydcdq5jH878IdG7xlL9lHVqrS8v3sBUqQgyR4xbpUgDnB5ZpYro6O"
                "xNnAzJ7RV6H"
                )
            )
        fs = conn.get_feature_store()
        ```

=== "Scala"

    !!! example "Connecting from Hopsworks"
        ```scala
        import com.logicalclocks.hsfs._
        val connection = HopsworksConnection.builder().build();
        val fs = connection.getFeatureStore();
        ```

    !!! example "Connecting from Databricks"
        TBD

    !!! example "Connecting from AWS SageMaker"
        The Scala client version of `hsfs` is not supported on AWS SageMaker,
        please use the Python client.

## Sharing a Feature Store

Connections are on a project-level, however, it is possible to share feature stores among projects, so even if you have a connection to one project, you can retireve a handle to any feature store shared with that project.

To share a feature store, you can follow these steps:

!!! info "Sharing a Feature Store"

    1. Open the project of the feature store that you would like to share on Hopsworks.
    2. Go to the *Data Set* browser and right click the `Featurestore.db` entry.
    3. Click *Share with*, then select *Project* and choose the project you wish to share the feature store with.
    4. Select the permissions level that the project user members should have on the feature store and click *Share*.
    4. Open the project you just shared the feature store with.
    5. Go to the *Data Sets* browser and there you should see the shared feature store as `[project_name_of_shared_feature_store]::Featurestore.db`. Click this entry, you will be asked to accept this shared Dataset, click *Accept*.
    7. You should now have access to this feature store from the other project.

<p align="center">
  <figure>
      <img src="../../assets/images/featurestore-sharing.png" width="500" alt="Sharing a feature store between projects">
    <figcaption>Sharing a feature store between projects</figcaption>
  </figure>
</p>

<p align="center">
  <figure>
      <img src="../../assets/images/featurestore-sharing-2.png" alt="Accepting a shared feature store">
    <figcaption>Accepting a shared feature store from a project</figcaption>
  </figure>
</p>

## Connection Handle

{{connection}}

## Methods

{{connection_methods}}
