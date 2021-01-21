# AWS SageMaker Integration

Connecting to the Feature Store from SageMaker requires setting up a Feature Store API key for SageMaker and installing the **HSFS** on SageMaker. This guide explains step by step how to connect to the Feature Store from SageMaker.

## Generate an API key

In Hopsworks, click on your *username* in the top-right corner and select *Settings* to open the user settings. Select *API keys*. Give the key a name and select the job, featurestore and project scopes before creating the key. Copy the key into your clipboard for the next step.

!!! success "Scopes"
    The API key should contain at least the following scopes:

    1. featurestore
    2. project
    3. job

<p align="center">
  <figure>
    <img src="../../assets/images/api-key.png" alt="Generate an API key on Hopsworks">
    <figcaption>API keys can be created in the User Settings on Hopsworks</figcaption>
  </figure>
</p>

!!! info
    You are only ably to retrieve the API key once. If you did not manage to copy it to your clipboard, delete it again and create a new one.

## Quickstart API key Argument

!!! hint "API key as Argument"
    To get started quickly, without saving the Hopsworks API in a secret storage, you can simply supply it as an argument when instantiating a connection:
    ```python hl_lines="6"
        import hsfs
        conn = hsfs.connection(
            host='my_instance',                 # DNS of your Feature Store instance
            port=443,                           # Port to reach your Hopsworks instance, defaults to 443
            project='my_project',               # Name of your Hopsworks Feature Store project
            api_key_value='apikey',             # The API key to authenticate with Hopsworks
            hostname_verification=True          # Disable for self-signed certificates
        )
        fs = conn.get_feature_store()           # Get the project's default feature store
    ```


## Store the API key on AWS

The API key now needs to be stored on AWS, so it can be retrieved from within SageMaker notebooks.

### Identify your SageMaker role

You need to know the IAM role used by your SageMaker instance to set up the API key for it. You can find it in the overview of your SageMaker notebook instance of the AWS Management Console.

In this example, the name of the role is **AmazonSageMaker-ExecutionRole-20190511T072435**.

<p align="center">
  <figure>
    <img src="../../assets/images/sagemaker-role.png" alt="Identify your SageMaker Role">
    <figcaption>The role is attached to your SageMaker notebook instance</figcaption>
  </figure>
</p>

### Store the API key

You have two options to make your API key accessible from SageMaker:

#### Option 1: Using the AWS Systems Manager Parameter Store

##### Store the API key in the AWS Systems Manager Parameter Store

1. In the AWS Management Console, ensure that your active region is the region you use for SageMaker.
2. Go to the AWS Systems Manager choose *Parameter Store* in the left navigation bar and select *Create Parameter*.
3. As name, enter `/hopsworks/role/[MY_SAGEMAKER_ROLE]/type/api-key` replacing `[MY_SAGEMAKER_ROLE]` with the AWS role used by the SageMaker instance that should access the Feature Store.
4. Select *Secure String* as type and *create the parameter*.

<p align="center">
  <figure>
    <img src="../../assets/images/parameter-store.png" alt="AWS Systems Manager Parameter Store">
    <figcaption>Store the API key in the AWS Systems Manager Parameter Store</figcaption>
  </figure>
</p>

##### Grant access to the Parameter Store from the SageMaker notebook role

1. In the AWS Management Console, go to *IAM*, select *Roles* and then the role that is used when creating SageMaker notebook instances.
2. Select *Add inline policy*.
3. Choose *Systems Manager* as service, expand the *Read access level* and check *GetParameter*.
4. Expand *Resources* and select *Add ARN*.
6. Enter the region of the Systems Manager as well as the name of the parameter **WITHOUT the leading slash** e.g. `hopsworks/role/[MY_SAGEMAKER_ROLE]/type/api-key` and click *Add*.
7. Click on *Review*, give the policy a name und click on *Create policy*.

<p align="center">
  <figure>
    <img src="../../assets/images/parameter-store-policy.png" alt="AWS Systems Manager Parameter Store Get Parameter Policy">
    <figcaption>Grant access to the Parameter Store from the SageMaker notebook role</figcaption>
  </figure>
</p>

#### Option 2: Using the AWS Secrets Manager

##### Store the API key in the AWS Secrets Manager

1. In the AWS Management Console, ensure that your active region is the region you use for SageMaker.
2. Go to the *AWS Secrets Manager* and select *Store new secret*.
3. Select *Other type of secrets* and add api-key as the key and paste the API key created in the previous step as the value.
4. Click next.

<p align="center">
  <figure>
    <img src="../../assets/images/secrets-manager-1.png" alt="AWS Systems Manager">
    <figcaption>Store the API key in the AWS Secrets Manager</figcaption>
  </figure>
</p>

5. As secret name, enter `hopsworks/role/[MY_SAGEMAKER_ROLE]` replacing `[MY_SAGEMAKER_ROLE]` with the AWS role used by the SageMaker instance that should access the Feature Store.
6. Select *next* twice and finally store the secret.
7. Then click on the secret in the secrets list and take note of the *Secret ARN*.

<p align="center">
  <figure>
    <img src="../../assets/images/secrets-manager-2.png" alt="AWS Systems Manager">
    <figcaption>Store the API key in the AWS Secrets Manager</figcaption>
  </figure>
</p>

##### Grant access to the SecretsManager to the SageMaker notebook role

1. In the AWS Management Console, go to *IAM*, select *Roles* and then the role that is used when creating SageMaker notebook instances.
2. Select *Add inline policy*.
3. Choose *Secrets Manager* as service, expand the *Read access* level and check *GetSecretValue*.
4. Expand *Resources* and select *Add ARN*.
5. Paste the *ARN* of the secret created in the previous step.
6. Click on *Review*, give the policy a name und click on *Create policy*.

<p align="center">
  <figure>
    <img src="../../assets/images/secrets-manager-policy.png" alt="AWS Systems Manager GetSecretValue policy">
    <figcaption>Grant access to the SecretsManager to the SageMaker notebook role</figcaption>
  </figure>
</p>

## Install **HSFS**

To be able to access the Hopsworks Feature Store, the `HSFS` Python library needs to be installed. One way of achieving this is by opening a Python notebook in SageMaker and installing the `HSFS` with a magic command and pip:

```
!pip install hsfs[hive]~=[HOPSWORKS_VERSION]
```

!!! attention "Hive Dependencies"

    By default, `HSFS` assumes Spark/EMR is used as execution engine and therefore Hive dependencies are not installed. Hence, on AWS SageMaker, if you are planning to use a regular Python Kernel **without Spark/EMR**, make sure to install the **"hive"** extra dependencies (`hsfs[hive]`).

!!! attention "Matching Hopsworks version"
    The **major version of `HSFS`** needs to match the **major version of Hopsworks**.


<p align="center">
    <figure>
        <img src="../../assets/images/hopsworks-version.png" alt="HSFS version needs to match the major version of Hopsworks">
        <figcaption>You find the Hopsworks version inside any of your Project's settings tab on Hopsworks</figcaption>
    </figure>
</p>

Note that the library will not be persistent. For information around how to permanently install a library to SageMaker, see [Install External Libraries and Kernels](https://docs.aws.amazon.com/sagemaker/latest/dg/nbi-add-external.html) in Notebook Instances.

## Connect to the Feature Store

You are now ready to connect to the Hopsworks Feature Store from SageMaker:

```python
import hsfs
conn = hsfs.connection(
    'my_instance',                      # DNS of your Feature Store instance
    443,                                # Port to reach your Hopsworks instance, defaults to 443
    'my_project',                       # Name of your Hopsworks Feature Store project
    secrets_store='secretsmanager',     # Either parameterstore or secretsmanager
    hostname_verification=True,         # Disable for self-signed certificates
    engine='hive'                       # Choose Hive as engine if you haven't set up AWS EMR
)
fs = conn.get_feature_store()           # Get the project's default feature store
```

!!! note "Engine"

    `HSFS` uses either Apache Spark or Apache Hive as an execution engine to perform queries against the feature store. Most AWS SageMaker Kernels have PySpark installed but are not connected to AWS EMR by default, hence, the `engine` option of the connection let's you overwrite the default behaviour. By default, `HSFS` will try to use Spark as engine if PySpark is available, however, if Spark/EMR is not configured, you will have to set the engine manually to `"hive"`. Please refer to the [EMR integration guide](emr/emr_configuration.md) to setup EMR with the Hopsworks Feature Store.


!!! info "Ports"

    If you have trouble connecting, please ensure that the Security Group of your Hopsworks instance on AWS is configured to allow incoming traffic from your SageMaker instance on ports 443, 9083 and 9085 (443,9083,9085). See [VPC Security Groups](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html) for more information. If your SageMaker instances are not in the same VPC as your Hopsworks instance and the Hopsworks instance is not accessible from the internet then you will need to configure [VPC Peering on AWS](https://docs.aws.amazon.com/vpc/latest/peering/what-is-vpc-peering.html).

## Next Steps

For more information about how to use the Feature Store, see the [Quickstart Guide](../quickstart.md).
