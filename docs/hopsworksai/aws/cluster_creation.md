# Getting started with Hopsworks.ai (AWS)
This guide goes into detail for each of the steps of the cluster creation in Hopsworks.ai

### Step 1 starting to create a cluster

In Hopsworks.ai, select *Create cluster*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-instance.png">
      <img src="../../../assets/images/hopsworksai/create-instance.png" alt="Create a Hopsworks cluster">
    </a>
    <figcaption>Create a Hopsworks cluster</figcaption>
  </figure>
</p>

### Setp 2 setting the General information

Select the *Region* in which you want your cluster to run (1), name your cluster (2).

Select the *Instance type* (3) and *Local storage* (4) size for the cluster *Head node*.

Select the number of workers you want to start the cluster with (5).
Select the *Instance type* (6) and *Local storage* size (7) for the *worker nodes*.

!!! note
    It is possible to add or remove workers once the cluster is running.

Enter the name of the *S3 bucket* (8) you want the cluster to store its data in, in *S3 bucket*.

!!! note
    The S3 bucket you are using must be empty.

Press *Next* (9):

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-instance-general.png">
      <img src="../../../assets/images/hopsworksai/aws/create-instance-general.png" alt="Create a Hopsworks cluster, general Information">
    </a>
    <figcaption>Create a Hopsworks cluster, general information</figcaption>
  </figure>
</p>

### Step 3 select an SSH key

When deploying clusters, Hopsworks.ai installs an ssh key on the cluster's instances so that you can access them if necessary.
Select the *SSH key* that you want to use to access cluster instances. For more detail on how to add a shh key in AWS refer to [Create an ssh key](getting_started.md#step-3-create-an-ssh-key)

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-ssh.png">
      <img src="../../../assets/images/hopsworksai/aws/connect-aws-ssh.png" alt="Choose SSH key">
    </a>
    <figcaption>Choose SSH key</figcaption>
  </figure>
</p>

### Step 4 select the Instance Profile:

To let the cluster instances access the S3 bucket we need to attach an *instance profile* to the virtual machines. In this step, you choose which profile to use. This profile needs to have access right to the *S3 bucket* you selected in [Step 2](#setp-2-setting-the-general-information). For more details on how to create the instance profile and give it access to the S3 bucket refer to [Creating an instance profile and giving it access to the bucket](getting_started.md#step-22-creating-an-instance-profile-and-giving-it-access-to-the-bucket)

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-profile.png">
      <img src="../../../assets/images/hopsworksai/aws/connect-aws-profile.png" alt="Choose the instance profile">
    </a>
    <figcaption>Choose the instance profile</figcaption>
  </figure>
</p>

### Step 5 Managed Containers:
Hopsworks can integrate with Amazon Elastic Kubernetes Service (EKS) and Amazon Elastic Container Registry (ECR) to launch Python jobs, Jupyter servers, and ML model servings on top of Amazon EKS. For more detail on how to set up this integration refer to [Integration with Amazon EKS and Amazon ECR](eks_ecr_integration.md).
<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-2.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-2.png" alt="Add EKS cluster name">
    </a>
    <figcaption>Add EKS cluster name</figcaption>
  </figure>
</p>

### Step 6 VPC selection
In this step, you can select the VPC which will be used by the Hopsworks cluster. You can either select an existing VPC or let Hopsworks.ai create one for you. If you decide to let Hopsworks.ai create the VPC for you, you can choose the CIDR block for this virtual network. 
Refer to [Create a VPC](restrictive_permissions.md#step-1-create-a-vpc) for more details on how to create your own VPC in AWS.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-aws-vpc.png">
      <img src="../../../assets/images/hopsworksai/aws/create-aws-vpc.png" alt="Choose VPC">
    </a>
    <figcaption>Choose a VPC</figcaption>
  </figure>
</p>

### Step 7 Availability Zone selection
If you selected an existing VPC in the previous step, this step lets you select which availability zone of this VPC to use.

If you did not select an existing virtual network in the previous step Hopsworks.ai will create an availability zone for you. You can choose the CIDR block this subnet will use.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-subnet.png">
      <img src="../../../assets/images/hopsworksai/aws/connect-aws-subnet.png" alt="Choose subnet">
    </a>
    <figcaption>Choose an availability zone</figcaption>
  </figure>
</p>

### Step 8 Security group selection
If you selected an existing VPC in the previous step, this step lets you select which security group to use.

!!! note
    For Hopsworks.ai to create the SSL certificates the security group needs to allow inbound traffic on port 80.

    If you intend to use the managed users option on your Hopsworks cluster you should also add a rule to open port 443.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-security-group.png">
      <img src="../../../assets/images/hopsworksai/aws/connect-aws-security-group.png" alt="Choose security group">
    </a>
    <figcaption>Choose security group</figcaption>
  </figure>
</p>

### Step 9 User management selection
In this step, you can choose which user management system to use. You have three choices: 

* *Managed*: Hopsworks.ai automatically adds and removes users from the Hopsworks cluster when you add and remove users from your organization.
* *LDAP*: integrate the cluster with your organization's LDAP/ActiveDirectory server.
* *Disabled*: let you manage users manually from within Hopsworks.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-user-management.png">
      <img src="../../../assets/images/hopsworksai/aws/aws-user-management.png" alt="Choose user management type">
    </a>
    <figcaption>Choose user management type</figcaption>
  </figure>
</p>

### Step 10 add tags to your instances.
In this step, you can define tags that will be added to the cluster virtual machines.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-tags.png">
      <img src="../../../assets/images/hopsworksai/aws/aws-tags.png" alt="Add tags">
    </a>
    <figcaption>Add tags</figcaption>
  </figure>
</p>

### Step 11 Review and create
Review all information and select *Create*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-review.png">
      <img src="../../../assets/images/hopsworksai/aws/connect-aws-review.png" alt="Review cluster information">
    </a>
    <figcaption>Review cluster information</figcaption>
  </figure>
</p>

The cluster will start. This will take a few minutes:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/booting.png">
      <img src="../../../assets/images/hopsworksai/booting.png" alt="Booting Hopsworks cluster">
    </a>
    <figcaption>Booting Hopsworks cluster</figcaption>
  </figure>
</p>

As soon as the cluster has started, you will be able to log in to your new Hopsworks cluster with the username and password provided. You will also be able to stop, restart, or terminate the cluster.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/running.png">
      <img src="../../../assets/images/hopsworksai/running.png" alt="Running Hopsworks cluster">
    </a>
    <figcaption>Running Hopsworks cluster</figcaption>
  </figure>
</p>