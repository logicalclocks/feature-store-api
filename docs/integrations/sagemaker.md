# AWS SageMaker Integration

Connecting to the Feature Store from SageMaker requires setting up a Feature Store API Key for SageMaker and installing the **HSFS** on SageMaker. This guide explains step by step how to connect to the Feature Store from SageMaker.

## Generating an API Key

In Hopsworks, click on your username in the top-right corner and select Settings to open the user settings. Select Api keys. Give the key a name and select the job, featurestore and project scopes before creating the key. Copy the key into your clipboard for the next step.

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

## Storing the API Key on AWS

The API Key now needs to be stored on AWS, so it can be retrieved from within SageMaker notebooks.

### Identifying your SageMaker role

You need to know the IAM role used by your SageMaker instance to set up the API Key for it. You can find it in the overview of your SageMaker notebook instance of the AWS Management Console.

In this example, the name of the role is **AmazonSageMaker-ExecutionRole-20190511T072435**.

<p align="center">
  <figure>
    <img src="../../assets/images/sagemaker-role.png" alt="Identifying your SageMaker Role">
    <figcaption>The role is attached to your SageMaker notebook instance</figcaption>
  </figure>
</p>

### Storing the API Key

You have two options to make your API Key accesible from SageMaker:

#### Option 1: Using the AWS Systems Manager Parameter Store

##### Storing the API Key in the AWS Systems Manager Parameter Store

1. In the AWS Management Console, ensure that your active region is the region you use for SageMaker.
2. Go to the AWS Systems Manager choose *Parameter Store* in the left navigation bar and select *Create Parameter*.
3. As name, enter `/hopsworks/role/[MY_SAGEMAKER_ROLE]/type/api-key` replacing `[MY_SAGEMAKER_ROLE]` with the AWS role used by the SageMaker instance that should access the Feature Store.
4. Select *Secure String* as type and *create the parameter*.

<p align="center">
  <figure>
    <img src="../../assets/images/parameter-store.png" alt="AWS Systems Manager Parameter Store">
    <figcaption>Storing the API Key in the AWS Systems Manager Parameter Store</figcaption>
  </figure>
</p>

##### Granting access to the Parameter Store from the SageMaker notebook role

1. In the AWS Management Console, go to *IAM*, select *Roles* and then the role that is used when creating SageMaker notebook instances.
2. Select *Add inline policy*.
3. Choose *Systems Manager* as service, expand the *Read access level* and check *GetParameter*.
4. Expand *Resources* and select *Add ARN*.
6. Enter the region of the Systems Manager as well as the name of the parameter **WITHOUT the leading slash** e.g. `hopsworks/role/[MY_SAGEMAKER_ROLE]/type/api-key` and click *Add*.
7. Click on *Review*, give the policy a name und click on *Create policy*.

<p align="center">
  <figure>
    <img src="../../assets/images/parameter-store-policy.png" alt="AWS Systems Manager Parameter Store Get Parameter Policy">
    <figcaption>Granting access to the Parameter Store from the SageMaker notebook role</figcaption>
  </figure>
</p>

#### Option 2: Using the AWS Secrets Manager

##### Storing the API Key in the AWS Secrets Manager

1. In the AWS Management Console, ensure that your active region is the region you use for SageMaker.
2. Go to the *AWS Secrets Manager* and select *Store new secret*.
3. Select *Other type of secrets* and add api-key as the key and paste the API key created in the previous step as the value.
4. Click next.

<p align="center">
  <figure>
    <img src="../../assets/images/secrets-manager-1.png" alt="AWS Systems Manager">
    <figcaption>Storing the API Key in the AWS Secrets Manager</figcaption>
  </figure>
</p>

5. As secret name, enter `hopsworks/role/[MY_SAGEMAKER_ROLE]` replacing `[MY_SAGEMAKER_ROLE]` with the AWS role used by the SageMaker instance that should access the Feature Store.
6. Select *next* twice and finally store the secret.
7. Then click on the secret in the secrets list and take note of the *Secret ARN*.

<p align="center">
  <figure>
    <img src="../../assets/images/secrets-manager-2.png" alt="AWS Systems Manager">
    <figcaption>Storing the API Key in the AWS Secrets Manager</figcaption>
  </figure>
</p>

##### Granting access to the SecretsManager to the SageMaker notebook role

1. In the AWS Management Console, go to *IAM*, select *Roles* and then the role that is used when creating SageMaker notebook instances.
2. Select *Add inline policy*.
3. Choose *Secrets Manager* as service, expand the *Read access* level and check *GetSecretValue*.
4. Expand *Resources* and select *Add ARN*.
5. Paste the *ARN* of the secret created in the previous step.
6. Click on *Review*, give the policy a name und click on *Create policy*.

<p align="center">
  <figure>
    <img src="../../assets/images/secrets-manager-policy.png" alt="AWS Systems Manager GetSecretValue policy">
    <figcaption>Granting access to the SecretsManager to the SageMaker notebook role</figcaption>
  </figure>
</p>

## Installing **HSFS**

To be able to access the Hopsworks Feature Store, the `HSFS` Python library needs to be installed. One way of achieving this is by opening a Python notebook in SageMaker and installing the `HSFS` with a magic command and pip:

```
!pip install hsfs~=[HOPSWORKS_VERSION]
```

!!! attention "Matching Hopsworks version"
    The **major version of `HSFS`** needs to match the **major version of Hopsworks**.


<p align="center">
    <figure>
        <img src="../../assets/images/hopsworks-version.png" alt="HSFS version needs to match the major version of Hopsworks">
        <figcaption>You find the Hopsworks version inside any of your Project's settings tab on Hopsworks</figcaption>
    </figure>
</p>

Note that the library will not be persistent. For information around how to permanently install a library to SageMaker, see [Install External Libraries and Kernels](https://docs.aws.amazon.com/sagemaker/latest/dg/nbi-add-external.html) in Notebook Instances.

## Connecting to the Feature Store

Your are now ready to connect to the Hopsworks Feature Store from SageMaker:

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

!!! info "Ports"

    If you have trouble connecting, then ensure that the Security Group of your Hopsworks instance on AWS is configured to allow incoming traffic from your SageMaker instance on ports 443, 9083 and 9085. See [VPC Security Groups](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html) for more information. If your SageMaker instances are not in the same VPC as your Hopsworks instance and the Hopsworks instance is not accessible from the internet then you will need to configure [VPC Peering on AWS](https://docs.aws.amazon.com/vpc/latest/peering/what-is-vpc-peering.html).

## Next Steps

For more information about how to connect, see the [Connection](../concepts/project.md) guide. Or continue with the Data Source guide to import your own data to the Feature Store.
