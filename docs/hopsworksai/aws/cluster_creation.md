# Getting started with Hopsworks.ai (AWS)
This guide goes into detail for each of the steps of the cluster creation in Hopsworks.ai

### Step 1 starting to create a cluster

In Hopsworks.ai, select *Create cluster*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-instance.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/create-instance.png" alt="Create a Hopsworks cluster">
    </a>
    <figcaption>Create a Hopsworks cluster</figcaption>
  </figure>
</p>

### Step 2 setting the General information

Select the *Region* in which you want your cluster to run (1), name your cluster (2).

Select the *Instance type* (3) and *Local storage* (4) size for the cluster *Head node*.

Enter the name of the *S3 bucket* (5) you want the cluster to store its data in, in *S3 bucket*.

!!! note
    The S3 bucket you are using must be empty.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-instance-general.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-instance-general.png" alt="Create a Hopsworks cluster, general Information">
    </a>
    <figcaption>Create a Hopsworks cluster, general information</figcaption>
  </figure>
</p>

### Step 3 workers configuration

In this step, you configure the workers. There are two possible setups static or autoscaling. In the static setup, the cluster has a fixed number of workers that you decide. You can then add and remove workers manually, for more details: [the documentation](../adding_removing_workers.md). In the autoscaling setup, you configure conditions to add and remove workers and the cluster will automatically add and remove workers depending on the demand.

#### Static workers configuration
You can set the static configuration by selecting *Disabled* in the first drop-down (1). Then you select the number of workers you want to start the cluster with (2). And, select the *Instance type* (3) and *Local storage* size (4) for the *worker nodes*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-instance-workers-static.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/create-instance-workers-static.png" alt="Create a Hopsworks cluster, static workers configuration">
    </a>
    <figcaption>Create a Hopsworks cluster, static workers configuration</figcaption>
  </figure>
</p>

#### Autoscaling workers configuration
You can set the autoscaling configuration by selecting enabled in the first drop-down (1). You then have access to a two parts form allowing you to configure the autoscaling. In the first part, you configure the autoscaling for general-purpose compute nodes. In the second part, you configure the autoscaling for nodes equipped with GPUs. In both parts you will have to set up the following:

1. The instance type you want to use. You can decide to not enable the autoscaling for GPU nodes by selecting *No GPU autoscale*.
2. The size of the instances' disk.
3. The minimum number of workers. 
4. The maximum number of workers.
5. The targeted number of standby workers. Setting some resources in standby ensures that there are always some free resources in your cluster. This ensures that requests for new resources are fulfilled promptly. You configure the standby by setting the amount of workers you want to be in standby. For example, if you set a value of *0.5* the system will start a new worker every time the aggregated free cluster resources drop below 50% of a worker's resources. If you set this value to 0 new workers will only be started when a job or notebook request the resources.
6. The time to wait before removing unused resources. One often starts a new computation shortly after finishing the previous one. To avoid having to wait for workers to stop and start between each computation it is recommended to wait before shutting down workers. Here you set the amount of time in seconds resources need to be unused before they get removed from the system.

!!! note
    The standby will not be taken into account if you set the minimum number of workers to 0 and no resources are used in the cluster. This ensures that the number of nodes can fall to 0 when no resources are used. The standby will start to take effect as soon as you start using resources.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-instance-workers-autoscale.png">
      <img style="border: 1px solid #000;width:700px;width:506px" src="../../../assets/images/hopsworksai/create-instance-workers-autoscale.png" alt="Create a Hopsworks cluster, autoscale workers configuration">
    </a>
    <figcaption>Create a Hopsworks cluster, autoscale workers configuration</figcaption>
  </figure>
</p>

### Step 4 select an SSH key

When deploying clusters, Hopsworks.ai installs an ssh key on the cluster's instances so that you can access them if necessary.
Select the *SSH key* that you want to use to access cluster instances. For more detail on how to add a shh key in AWS refer to [Create an ssh key](getting_started.md#step-4-create-an-ssh-key)

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-ssh.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/connect-aws-ssh.png" alt="Choose SSH key">
    </a>
    <figcaption>Choose SSH key</figcaption>
  </figure>
</p>

### Step 5 select the Instance Profile

To let the cluster instances access the S3 bucket we need to attach an *instance profile* to the virtual machines. In this step, you choose which profile to use. This profile needs to have access right to the *S3 bucket* you selected in [Step 2](#step-2-setting-the-general-information). For more details on how to create the instance profile and give it access to the S3 bucket refer to [Creating an instance profile and giving it access to the bucket](getting_started.md#step-22-creating-an-instance-profile-and-giving-it-access-to-the-bucket)

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-profile.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/connect-aws-profile.png" alt="Choose the instance profile">
    </a>
    <figcaption>Choose the instance profile</figcaption>
  </figure>
</p>

### Step 6 set the backup retention policy

!!! note
    This step is only accessible to enterprise users.

To back up the S3 bucket data when taking a cluster backup we need to set a retention policy for S3. In this step, you choose the retention period in days. You can deactivate the retention policy by setting this value to 0 but this will block you from taking any backup of your cluster.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-backup.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-backup.png" alt="Choose the backup retention policy">
    </a>
    <figcaption>Choose the backup retention policy</figcaption>
  </figure>
</p>

### Step 7 Managed Containers
Hopsworks can integrate with Amazon Elastic Kubernetes Service (EKS) and Amazon Elastic Container Registry (ECR) to launch Python jobs, Jupyter servers, and ML model servings on top of Amazon EKS. For more detail on how to set up this integration refer to [Integration with Amazon EKS and Amazon ECR](eks_ecr_integration.md).
<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-2.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-2.png" alt="Add EKS cluster name">
    </a>
    <figcaption>Add EKS cluster name</figcaption>
  </figure>
</p>

### Step 8 VPC selection
In this step, you can select the VPC which will be used by the Hopsworks cluster. You can either select an existing VPC or let Hopsworks.ai create one for you. If you decide to let Hopsworks.ai create the VPC for you, you can choose the CIDR block for this virtual network. 
Refer to [Create a VPC](restrictive_permissions.md#step-1-create-a-vpc) for more details on how to create your own VPC in AWS.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/create-aws-vpc.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/create-aws-vpc.png" alt="Choose VPC">
    </a>
    <figcaption>Choose a VPC</figcaption>
  </figure>
</p>

### Step 9 Availability Zone selection
If you selected an existing VPC in the previous step, this step lets you select which availability zone of this VPC to use.

If you did not select an existing virtual network in the previous step Hopsworks.ai will create an availability zone for you. You can choose the CIDR block this subnet will use.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-subnet.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/connect-aws-subnet.png" alt="Choose subnet">
    </a>
    <figcaption>Choose an availability zone</figcaption>
  </figure>
</p>

### Step 10 Security group selection
If you selected an existing VPC in the previous step, this step lets you select which security group to use.

!!! note
    For Hopsworks.ai to create the SSL certificates the security group needs to allow inbound traffic on port 80.

    If you intend to use the managed users option on your Hopsworks cluster you should also add a rule to open port 443.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/connect-aws-security-group.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/connect-aws-security-group.png" alt="Choose security group">
    </a>
    <figcaption>Choose security group</figcaption>
  </figure>
</p>

### Step 11 User management selection
In this step, you can choose which user management system to use. You have three choices: 

* *Managed*: Hopsworks.ai automatically adds and removes users from the Hopsworks cluster when you add and remove users from your organization.
* *LDAP*: integrate the cluster with your organization's LDAP/ActiveDirectory server.
* *Disabled*: let you manage users manually from within Hopsworks.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-user-management.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/aws-user-management.png" alt="Choose user management type">
    </a>
    <figcaption>Choose user management type</figcaption>
  </figure>
</p>

### Step 12 Managed RonDB
Hopsworks.ai users have the option to create a cluster with dedicated VMs running [RonDB](https://www.rondb.com/).

For details on how to configure RonDB check our guide [here](../rondb.md).

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/rondb/rondb_enable.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/rondb/rondb_enable.png" alt="Enable Managed RonDB">
    </a>
    <figcaption>Enable Managed RonDB</figcaption>
  </figure>
</p>

If you need this feature and don't see this step please contact [us](mailto:sales@logicalclocks.com).

### Step 13 add tags to your instances.
In this step, you can define tags that will be added to the cluster virtual machines.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-tags.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/aws/aws-tags.png" alt="Add tags">
    </a>
    <figcaption>Add tags</figcaption>
  </figure>
</p>

### Step 14 Review and create
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
