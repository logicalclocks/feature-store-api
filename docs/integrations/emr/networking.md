# Networking

In order for Spark to communicate with the Hopsworks Feature Store from EMR, networking needs to be set up correctly. This includes deploying the Hopsworks Feature Store to either the same VPC or enable VPC peering between the VPC of the EMR cluster and the Hopsworks Feature Store.

## Step 1: Ensure network connectivity

The DataFrame API needs to be able to connect directly to the IP on which the Feature Store is listening.
This means that if you deploy the Feature Store on AWS you will either need to deploy the Feature Store in the same VPC as your EMR
cluster or to set up [VPC Peering](https://docs.aws.amazon.com/vpc/latest/peering/create-vpc-peering-connection.html) between your EMR VPC and the Feature Store VPC.

**Option 1: Deploy the Feature Store in the EMR VPC**

When deploying the Hopsworks Feature Store, select the EMR *VPC* and *Availability Zone* as the VPC and Availability Zone of your Feature Store.
Identify your EMR VPC in the Summary of your EMR cluster:

<p align="center">
  <figure>
    <a  href="../../../assets/images/emr/emr_vpc_0.png">
      <img src="../../../assets/images/emr/emr_vpc_0.png" alt="Identify the EMR VPC">
    </a>
    <figcaption>Identify the EMR VPC</figcaption>
  </figure>
</p>

<p align="center">
  <figure>
    <a  href="../../../assets/images/emr/emr_vpc_1.png">
      <img src="../../../assets/images/emr/emr_vpc_1.png" alt="Identify the EMR VPC">
    </a>
    <figcaption>Identify the EMR VPC</figcaption>
  </figure>
</p>

!!! info "Hopsworks installer"
    If you are performing an installation using the [Hopsworks installer script](https://hopsworks.readthedocs.io/en/stable/getting_started/installation_guide/platforms/hopsworks-installer.html), ensure that the virtual machines you install Hopsworks on are deployed in the EMR VPC.

!!! info "Hopsworks.ai"
    If you are on **Hopsworks.ai**, you can directly deploy Hopsworks to the EMR VPC, by simply selecting it at the [VPC selection step during cluster creation](../../hopsworksai/aws/cluster_creation.md#step-8-vpc-selection).

**Option 2: Set up VPC peering**

Follow the guide [VPC Peering](https://docs.aws.amazon.com/vpc/latest/peering/create-vpc-peering-connection.html) to set up VPC peering between the Feature Store and EMR. Get your Feature Store *VPC ID* and *CIDR* by searching for the Feature Store VPC in the AWS Management Console:

!!! info "Hopsworks.ai"
    On **Hopsworks.ai**, the VPC is shown in the cluster details.

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/aws/hopsworks_vpc.png">
      <img src="../../../assets/images/databricks/aws/hopsworks_vpc.png" alt="Identify the Feature Store VPC">
    </a>
    <figcaption>Identify the Feature Store VPC</figcaption>
  </figure>
</p>

## Step 2: Configure the Security Group

The Feature Store *Security Group* needs to be configured to allow traffic from your EMR clusters to be able to connect to the Feature Store.

!!! note "Hopsworks.ai"
    If you deployed your Hopsworks Feature Store with Hopsworks.ai, you only need to enable [outside access of the Feature Store services](../../../hopsworksai/aws/getting_started/#step-6-outside-access-to-the-feature-store).

Open your feature store instance under EC2 in the AWS Management Console and ensure that ports *443*, *3306*, *9083*, *9085*, *8020* and *30010* (443,3306,8020,30010,9083,9085) are reachable
from the EMR Security Group:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/aws/databricks_security_group_overview.png">
      <img src="../../../assets/images/databricks/aws/databricks_security_group_overview.png" alt="Hopsworks Feature Store Security Group">
    </a>
    <figcaption>Hopsworks Feature Store Security Group</figcaption>
  </figure>
</p>

Connectivity from the EMR Security Group can be allowed by opening the Security Group, adding a port to the Inbound rules and setting the EMR master and core security group as source:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/aws/databricks_security_group_details.png">
      <img src="../../../assets/images/databricks/aws/databricks_security_group_details.png" alt="Hopsworks Feature Store Security Group details">
    </a>
    <figcaption>Hopsworks Feature Store Security Group details</figcaption>
  </figure>
</p>

You can find your EMR security groups in the EMR cluster summary:

<p align="center">
  <figure>
    <a  href="../../../assets/images/emr/emr_security_group.png">
      <img src="../../../assets/images/emr/emr_security_group.png" alt="EMR Security Groups">
    </a>
    <figcaption>EMR Security Groups</figcaption>
  </figure>
</p>

## Next Steps

Continue with the [Configure EMR for the Hopsworks Feature Store](emr_configuration.md), in order to be able to use the Hopsworks Feature Store.
