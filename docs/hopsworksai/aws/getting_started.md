# Getting started with Hopsworks.ai (AWS)

Hopsworks.ai is our managed platform for running Hopsworks and the Feature Store
in the cloud. It integrates seamlessly with third party platforms such as Databricks,
SageMaker and KubeFlow. This guide shows how to set up Hopsworks.ai with your organization's AWS account.

## Step 1: Connecting your AWS account

Hopsworks.ai deploys Hopsworks clusters to your AWS account. To enable this you have to
give us permission to do so. This can be either achieved by using AWS cross-account roles or
AWS access keys. We strongly recommend the usage of cross-account roles whenever possible due
to security reasons.

### Option 1: Using AWS Cross-Account Roles

To create a cross-account role for Hopsworks.ai, you need our AWS account id and the external
id we created for you. You can find this information on the first screen of the cross-account
configuration flow. Take note of the account id and external id and go to the *Roles* section
of the *IAM* service in the AWS Management Console and select *Create role*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-role-instructions.png">
      <img src="../../../assets/images/hopsworksai/aws/create-role-instructions.png" alt="Creating the cross-account role instructions">
    </a>
    <figcaption>Creating the cross-account role instructions</figcaption>
  </figure>
</p>

Select *Another AWS account* as trusted entity and fill in our AWS account id and the external
id generated for you:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-role-aws-step-1.png">
      <img src="../../../assets/images/hopsworksai/aws/create-role-aws-step-1.png" alt="Creating the cross-account role step 1">
    </a>
    <figcaption>Creating the cross-account role step 1</figcaption>
  </figure>
</p>

Go to the last step of the wizard, name the role and create it:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-role-aws-step-2.png">
      <img src="../../../assets/images/hopsworksai/aws/create-role-aws-step-2.png" alt="Creating the cross-account role step 1">
    </a>
    <figcaption>Creating the cross-account role step 2</figcaption>
  </figure>
</p>

As a next step, you need to create an access policy to give Hopsworks.ai permissions to manage
clusters in your organization's AWS account. By default, Hopsworks.ai is automating all steps required to launch
a new Hopsworks cluster. If you want to limit the required AWS permissions, see [restrictive-permissions](restrictive_permissions.md).

Copy the permission JSON from the instructions:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-instructions.png">
      <img src="../../../assets/images/hopsworksai/aws/role-permissions-instructions.png" alt="Adding the policy instructions">
    </a>
    <figcaption>Adding the policy instructions</figcaption>
  </figure>
</p>

Identify your newly created cross-account role in the *Roles* section of the *IAM* service in the
AWS Management Console and select *Add inline policy*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-1.png">
      <img src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-1.png" alt="Adding the inline policy step 1">
    </a>
    <figcaption>Adding the inline policy step 1</figcaption>
  </figure>
</p>

Replace the JSON policy with the JSON from our instructions and continue in the wizard:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-2.png">
      <img src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-2.png" alt="Adding the inline policy step 2">
    </a>
    <figcaption>Adding the inline policy step 2</figcaption>
  </figure>
</p>

Name and create the policy:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-3.png">
      <img src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-3.png" alt="Adding the inline policy step 3">
    </a>
    <figcaption>Adding the inline policy step 3</figcaption>
  </figure>
</p>

Copy the *Role ARN* from the summary of your cross-account role:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-4.png">
      <img src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-4.png" alt="Adding the inline policy step 4">
    </a>
    <figcaption>Adding the inline policy step 4</figcaption>
  </figure>
</p>

Paste the *Role ARN* into Hopsworks.ai and select *Configure*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/save-role.png">
      <img src="../../../assets/images/hopsworksai/aws/save-role.png" alt="Saving the cross-account role">
    </a>
    <figcaption>Saving the cross-account role</figcaption>
  </figure>
</p>

### Option 2: Using AWS Access Keys

You can either create a new IAM user or use an existing IAM user to create access keys for Hopsworks.ai.
If you want to create a new IAM user, see [Creating an IAM User in Your AWS Account](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html).

!!! warning

    We recommend using Cross-Account Roles instead of Access Keys whenever possible, see [Option 1: Using AWS Cross-Account Roles](#option-1-using-aws-cross-account-roles).

Hopsworks.ai requires a set of permissions to be able to launch clusters in your AWS account.
The permissions can be granted by attaching an access policy to your IAM user.
By default, Hopsworks.ai is automating all steps required to launch a new Hopsworks cluster.
If you want to limit the required AWS permissions, see [restrictive-permissions](restrictive_permissions.md).

The required permissions are shown in the instructions. Copy them if you want to create a new access policy:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/access-key-permissions-instructions.png">
      <img src="../../../assets/images/hopsworksai/aws/access-key-permissions-instructions.png" alt="Configuring access key instructions">
    </a>
    <figcaption>Configuring access key instructions</figcaption>
  </figure>
</p>

Add a new *Inline policy* to your AWS user:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/access-keys-aws-step-1.png">
      <img src="../../../assets/images/hopsworksai/aws/access-keys-aws-step-1.png" alt="Configuring the access key on AWS step 1">
    </a>
    <figcaption>Configuring the access key on AWS step 1</figcaption>
  </figure>
</p>

Replace the JSON policy with the JSON from our instructions and continue in the wizard:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-2.png">
      <img src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-2.png" alt="Adding the inline policy step 2">
    </a>
    <figcaption>Adding the inline policy step 2</figcaption>
  </figure>
</p>

Name and create the policy:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-3.png">
      <img src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-3.png" alt="Adding the inline policy step 3">
    </a>
    <figcaption>Adding the inline policy step 3</figcaption>
  </figure>
</p>

In the overview of your IAM user, select *Create access key*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/access-keys-aws-step-2.png">
      <img src="../../../assets/images/hopsworksai/aws/access-keys-aws-step-2.png" alt="Configuring the access key on AWS step 2">
    </a>
    <figcaption>Configuring the access key on AWS step 2</figcaption>
  </figure>
</p>

Copy the *Access Key ID* and the *Secret Access Key*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/access-keys-aws-step-3.png">
      <img src="../../../assets/images/hopsworksai/aws/access-keys-aws-step-3.png" alt="Configuring the access key on AWS step 3">
    </a>
    <figcaption>Configuring the access key on AWS step 3</figcaption>
  </figure>
</p>

Paste the *Access Key ID* and the *Secret Access Key* into Hopsworks.ai and select *Configure*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/save-access-key.png">
      <img src="../../../assets/images/hopsworksai/aws/save-access-key.png" alt="Saving the access key pair">
    </a>
    <figcaption>Saving the access key pair</figcaption>
  </figure>
</p>

## Step 2: Deploying a Hopsworks cluster

In Hopsworks.ai, select *Create cluster*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-instance.png">
      <img src="../../../assets/images/hopsworksai/create-instance.png" alt="Create a Hopsworks cluster">
    </a>
    <figcaption>Create a Hopsworks cluster</figcaption>
  </figure>
</p>

Configure the cluster by selecting the location, instance type and optionally the VPC,
subnet and security group. Select *Deploy*.

!!! note "SSH key configuration"
    We recommend that you always configure a SSH key under advanced options to ensure you can troubleshoot the cluster if necessary.

{!hopsworksai/aws/s3_permissions.md!}

The cluster will start. This might take a couple of minutes:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/booting.png">
      <img src="../../../assets/images/hopsworksai/booting.png" alt="Booting Hopsworks cluster">
    </a>
    <figcaption>Booting Hopsworks cluster</figcaption>
  </figure>
</p>

As soon as the cluster has started, you will be able to log in to your new Hopsworks cluster with the username
and password provided. You will also able to stop or terminate the cluster.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/running.png">
      <img src="../../../assets/images/hopsworksai/running.png" alt="Running Hopsworks cluster">
    </a>
    <figcaption>Running Hopsworks cluster</figcaption>
  </figure>
</p>

## Step 3: Outside Access to the Feature Store

By default, only the Hopsworks UI is made available to clients on external networks, like the Internet.
To integrate with external platforms and access APIs for services such as the Feature Store, you have to open the service's ports.

Open ports by going to *Services* tab, selecting a service and pressing *Update*. This will update the *Security Group* attached to the Hopsworks cluster to allow incoming traffic on the relevant ports.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/open-ports.png">
      <img src="../../../assets/images/hopsworksai/open-ports.png" alt="Outside Access to the Feature Store">
    </a>
    <figcaption>Outside Access to the Feature Store</figcaption>
  </figure>
</p>

## Step 4: Next steps

Check out our other guides for how to get started with Hopsworks and the Feature Store:

* Get started with the [Hopsworks Feature Store](../../quickstart.md)
* Get started with Machine Learning on Hopsworks: [HopsML](https://hopsworks.readthedocs.io/en/stable/hopsml/index.html#hops-ml)
* Get started with Hopsworks: [User Guide](https://hopsworks.readthedocs.io/en/stable/user_guide/user_guide.html#userguide)
* Code examples and notebooks: [hops-examples](https://github.com/logicalclocks/hops-examples)
