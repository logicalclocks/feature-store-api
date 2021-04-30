# Getting started with Hopsworks.ai (AWS)

Hopsworks.ai is our managed platform for running Hopsworks and the Feature Store
in the cloud. It integrates seamlessly with third-party platforms such as Databricks,
SageMaker and KubeFlow. This guide shows how to set up Hopsworks.ai with your organization's AWS account.

## Step 1: Connecting your AWS account

Hopsworks.ai deploys Hopsworks clusters to your AWS account. To enable this you have to
permit us to do so. This can be either achieved by using AWS cross-account roles or
AWS access keys. We strongly recommend the usage of cross-account roles whenever possible due
to security reasons.

### Option 1: Using AWS Cross-Account Roles

<p align="center">
  <iframe
    title="Azure information video"
    style="width:700px; height: 370px;"
    src="https://www.youtube.com/embed/DLMBdA8d9nU"
    frameBorder="0"
    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
    allowFullScreen
  >
  </iframe>
</p>

To create a cross-account role for Hopsworks.ai, you need our AWS account id and the external
id we created for you. You can find this information on the first screen of the cross-account
configuration flow. Take note of the account id and external id and go to the *Roles* section
of the *IAM* service in the AWS Management Console and select *Create role*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-role-instructions.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-role-instructions.png" alt="Creating the cross-account role instructions">
    </a>
    <figcaption>Creating the cross-account role instructions</figcaption>
  </figure>
</p>

Select *Another AWS account* as trusted entity and fill in our AWS account id and the external
id generated for you:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-role-aws-step-1.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-role-aws-step-1.png" alt="Creating the cross-account role step 1">
    </a>
    <figcaption>Creating the cross-account role step 1</figcaption>
  </figure>
</p>

Go to the last step of the wizard, name the role and create it:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-role-aws-step-2.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-role-aws-step-2.png" alt="Creating the cross-account role step 1">
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
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/role-permissions-instructions.png" alt="Adding the policy instructions">
    </a>
    <figcaption>Adding the policy instructions</figcaption>
  </figure>
</p>

Identify your newly created cross-account role in the *Roles* section of the *IAM* service in the
AWS Management Console and select *Add inline policy*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-1.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-1.png" alt="Adding the inline policy step 1">
    </a>
    <figcaption>Adding the inline policy step 1</figcaption>
  </figure>
</p>

Replace the JSON policy with the JSON from our instructions and continue in the wizard:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-2.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-2.png" alt="Adding the inline policy step 2">
    </a>
    <figcaption>Adding the inline policy step 2</figcaption>
  </figure>
</p>

Name and create the policy:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-3.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-3.png" alt="Adding the inline policy step 3">
    </a>
    <figcaption>Adding the inline policy step 3</figcaption>
  </figure>
</p>

Copy the *Role ARN* from the summary of your cross-account role:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-4.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-4.png" alt="Adding the inline policy step 4">
    </a>
    <figcaption>Adding the inline policy step 4</figcaption>
  </figure>
</p>

Paste the *Role ARN* into Hopsworks.ai and click on *Finish*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/save-role.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/save-role.png" alt="Saving the cross-account role">
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
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/access-key-permissions-instructions.png" alt="Configuring access key instructions">
    </a>
    <figcaption>Configuring access key instructions</figcaption>
  </figure>
</p>

Add a new *Inline policy* to your AWS user:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/access-keys-aws-step-1.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/access-keys-aws-step-1.png" alt="Configuring the access key on AWS step 1">
    </a>
    <figcaption>Configuring the access key on AWS step 1</figcaption>
  </figure>
</p>

Replace the JSON policy with the JSON from our instructions and continue in the wizard:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-2.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-2.png" alt="Adding the inline policy step 2">
    </a>
    <figcaption>Adding the inline policy step 2</figcaption>
  </figure>
</p>

Name and create the policy:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-3.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/role-permissions-aws-step-3.png" alt="Adding the inline policy step 3">
    </a>
    <figcaption>Adding the inline policy step 3</figcaption>
  </figure>
</p>

In the overview of your IAM user, select *Create access key*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/access-keys-aws-step-2.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/access-keys-aws-step-2.png" alt="Configuring the access key on AWS step 2">
    </a>
    <figcaption>Configuring the access key on AWS step 2</figcaption>
  </figure>
</p>

Copy the *Access Key ID* and the *Secret Access Key*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/access-keys-aws-step-3.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/access-keys-aws-step-3.png" alt="Configuring the access key on AWS step 3">
    </a>
    <figcaption>Configuring the access key on AWS step 3</figcaption>
  </figure>
</p>

Paste the *Access Key ID* and the *Secret Access Key* into Hopsworks.ai and click on *Finish*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/save-access-key.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/save-access-key.png" alt="Saving the access key pair">
    </a>
    <figcaption>Saving the access key pair</figcaption>
  </figure>
</p>

## Step 2: Creating Instance profile

Hopsworks cluster nodes need access to certain resources such as S3 bucket and CloudWatch.

Follow the instructions in this guide to create an IAM instance profile with access to your S3 bucket: [Guide](https://docs.aws.amazon.com/codedeploy/latest/userguide/getting-started-create-iam-instance-profile.html)

When creating the policy, paste the following in the JSON tab.
{!hopsworksai/aws/instance_profile_permissions.md!}

## Step 3: Creating storage

The Hopsworks clusters deployed by hopsworks.ai store their data in an S3 bucket in your AWS account.
To enable this you need to create an S3 bucket and an instance profile to give cluster nodes access to the bucket.

Proceed to the [S3 Management Console](https://s3.console.aws.amazon.com/s3/home) and click on *Create bucket*:
<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-s3-bucket-1.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-s3-bucket-1.png" alt="Create an S3 bucket">
    </a>
    <figcaption>Create an S3 bucket</figcaption>
  </figure>
</p>

Name your bucket and select the region where your Hopsworks cluster will run. Click on *Create bucket* at the bottom of the page.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-s3-bucket-2.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-s3-bucket-2.png" alt="Create an S3 bucket">
    </a>
    <figcaption>Create an S3 bucket</figcaption>
  </figure>
</p>

## Step 4: Create an SSH key
When deploying clusters, Hopsworks.ai installs an ssh key on the cluster's instances so that you can access them if necessary. For this purpose, you need to add an ssh key to your AWS EC2 environment. This can be done in two ways: [creating a new key pair](#step-31-create-a-new-key-pair) or [importing an existing key pair](#step-32-import-a-key-pair).

### Step 4.1: Create a new key pair

Proceed to [Key pairs in the EC2 console](https://us-east-2.console.aws.amazon.com/ec2/v2/home?#KeyPairs) and click on *Create key pair*
<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-key-pair.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-key-pair.png" alt="Create a key pair">
    </a>
    <figcaption>Create a key pair</figcaption>
  </figure>
</p>

Name your key, select the file format you prefer and click on *Create key pair*.
<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-key-pair-2.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-key-pair-2.png" alt="Create a key pair">
    </a>
    <figcaption>Create a key pair</figcaption>
  </figure>
</p>

### Step 4.2: Import a key pair
Proceed to [Key pairs in the EC2 console](https://us-east-2.console.aws.amazon.com/ec2/v2/home?#KeyPairs), click on *Action* and click on *Import key pair*
<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/import-key-pair.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/import-key-pair.png" alt="Import a key pair">
    </a>
    <figcaption>Import a key pair</figcaption>
  </figure>
</p>

Name your key pair, upload your public key and click on *Import key pair*.
<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/import-key-pair-2.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/import-key-pair-2.png" alt="Import a key pair">
    </a>
    <figcaption>Import a key pair</figcaption>
  </figure>
</p>

## Step 5: Deploying a Hopsworks cluster

In Hopsworks.ai, select *Create cluster*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-instance.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/create-instance.png" alt="Create a Hopsworks cluster">
    </a>
    <figcaption>Create a Hopsworks cluster</figcaption>
  </figure>
</p>

Select the *Region* in which you want your cluster to run (1), name your cluster (2).

Select the *Instance type* (3) and *Local storage* (4) size for the cluster *Head node*.

Enter the name of the *S3 bucket* (5) you created above in *S3 bucket*.

!!! note
    The S3 bucket you are using must be empty.

Press *Next*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-instance-general.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-instance-general.png" alt="Create a Hopsworks cluster, general Information">
    </a>
    <figcaption>Create a Hopsworks cluster, general information</figcaption>
  </figure>
</p>


Select the number of workers you want to start the cluster with (2).
Select the *Instance type* (3) and *Local storage* size (4) for the *worker nodes*.

!!! note
    It is possible to [add or remove workers](../adding_removing_workers.md) or to [enable autoscaling](../autoscaling.md) once the cluster is running.

Press *Next*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-instance-workers-static.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/create-instance-workers-static.png" alt="Create a Hopsworks cluster, static workers configuration">
    </a>
    <figcaption>Create a Hopsworks cluster, static workers configuration</figcaption>
  </figure>
</p>

Select the *SSH key* that you want to use to access cluster instances:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-ssh.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/connect-aws-ssh.png" alt="Choose SSH key">
    </a>
    <figcaption>Choose SSH key</figcaption>
  </figure>
</p>

Select the *Instance Profile* that you created above and click on *Review and Submit*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-profile.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/connect-aws-profile.png" alt="Choose the instance profile">
    </a>
    <figcaption>Choose the instance profile</figcaption>
  </figure>
</p>

!!! note
    If you are an enterprise user you will have one more step before being able to click on *Review and Create*. In this step, you will be asked to set the backups retention policy. More details about this step [here](./cluster_creation.md#step-6-set-the-backup-retention-policy)

Review all information and select *Create*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-review.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/connect-aws-review.png" alt="Review cluster information">
    </a>
    <figcaption>Review cluster information</figcaption>
  </figure>
</p>

The cluster will start. This will take a few minutes:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/booting.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/booting.png" alt="Booting Hopsworks cluster">
    </a>
    <figcaption>Booting Hopsworks cluster</figcaption>
  </figure>
</p>

As soon as the cluster has started, you will be able to log in to your new Hopsworks cluster with the username and password provided. You will also be able to stop, restart, or terminate the cluster.


<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/running.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/running.png" alt="Running Hopsworks cluster">
    </a>
    <figcaption>Running Hopsworks cluster</figcaption>
  </figure>
</p>

## Step 6: Outside Access to the Feature Store

By default, only the Hopsworks UI is made available to clients on external networks, like the Internet.
To integrate with external platforms and access APIs for services such as the Feature Store, you have to open the service's ports.

Open ports by going to the *Services* tab, selecting a service, and pressing *Update*. This will update the *Security Group* attached to the Hopsworks cluster to allow incoming traffic on the relevant ports.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/open-ports.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/open-ports.png" alt="Outside Access to the Feature Store">
    </a>
    <figcaption>Outside Access to the Feature Store</figcaption>
  </figure>
</p>

## Step 7: Next steps

Check out our other guides for how to get started with Hopsworks and the Feature Store:

* Get started with the [Hopsworks Feature Store](../../quickstart.md)
* Get started with Machine Learning on Hopsworks: [HopsML](https://hopsworks.readthedocs.io/en/stable/hopsml/index.html#hops-ml)
* Get started with Hopsworks: [User Guide](https://hopsworks.readthedocs.io/en/stable/user_guide/user_guide.html#userguide)
* Code examples and notebooks: [hops-examples](https://github.com/logicalclocks/hops-examples)
