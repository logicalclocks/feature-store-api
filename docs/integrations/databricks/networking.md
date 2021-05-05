# Networking

In order for Spark to communicate with the Feature Store from Databricks, networking needs to be set up correctly. This includes deploying the Hopsworks Instance to either the same VPC or enable VPC/VNet peering between the VPC/VNet of the Databricks Cluster and the Hopsworks Cluster.

## AWS

### Step 1: Ensure network connectivity

The DataFrame API needs to be able to connect directly to the IP on which the Feature Store is listening.
This means that if you deploy the Feature Store on AWS you will either need to deploy the Feature Store in the same VPC as your Databricks
cluster or to set up [VPC Peering](https://docs.databricks.com/administration-guide/cloud-configurations/aws/vpc-peering.html) between your Databricks VPC and the Feature Store VPC.

**Option 1: Deploy the Feature Store in the Databricks VPC**

When you deploy the Feature Store Hopsworks instance, select the Databricks *VPC* and *Availability Zone* as the VPC and Availability Zone of your Feature Store cluster.
Identify your Databricks VPC by searching for VPCs containing Databricks in their name in your Databricks AWS region in the AWS Management Console:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/aws/databricks_vpc.png">
      <img src="../../../assets/images/databricks/aws/databricks_vpc.png" alt="Identify the Databricks VPC">
    </a>
    <figcaption>Identify the Databricks VPC</figcaption>
  </figure>
</p>

!!! info "Hopsworks installer"
    If you are performing an installation using the [Hopsworks installer script](https://hopsworks.readthedocs.io/en/stable/getting_started/installation_guide/platforms/hopsworks-installer.html), ensure that the virtual machines you install Hopsworks on are deployed in the EMR VPC.

!!! info "Hopsworks.ai"
    If you are working on **Hopsworks.ai**, you can directly deploy the Hopsworks instance to the Databricks VPC, by simply selecting it at the [VPC selection step during cluster creation](../../hopsworksai/aws/cluster_creation.md#step-8-vpc-selection).

**Option 2: Set up VPC peering**

Follow the guide [VPC Peering](https://docs.databricks.com/administration-guide/cloud-configurations/aws/vpc-peering.html) to set up VPC peering between the Feature Store cluster and Databricks. Get your Feature Store *VPC ID* and *CIDR* by searching for thr Feature Store VPC in the AWS Management Console:

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

### Step 2: Configure the Security Group

The Feature Store *Security Group* needs to be configured to allow traffic from your Databricks clusters to be able to connect to the Feature Store.

!!! note "Hopsworks.ai"
    If you deployed your Hopsworks Feature Store with Hopsworks.ai, you only need to enable [outside access of the Feature Store services](../../../hopsworksai/aws/getting_started/#step-6-outside-access-to-the-feature-store).

Open your feature store instance under EC2 in the AWS Management Console and ensure that ports *443*, *3306*, *9083*, *9085*, *8020* and *30010* are reachable from the Databricks Security Group:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/aws/databricks_security_group_overview.png">
      <img src="../../../assets/images/databricks/aws/databricks_security_group_overview.png" alt="Hopsworks Feature Store Security Group">
    </a>
    <figcaption>Hopsworks Feature Store Security Group</figcaption>
  </figure>
</p>

Connectivity from the Databricks Security Group can be allowed by opening the Security Group, adding a port to the Inbound rules and searching for *dbe-worker* in the source field. Selecting any of the *dbe-worker* Security Groups will be sufficient:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/aws/databricks_security_group_details.png">
      <img src="../../../assets/images/databricks/aws/databricks_security_group_details.png" alt="Hopsworks Feature Store Security Group details">
    </a>
    <figcaption>Hopsworks Feature Store Security Group details</figcaption>
  </figure>
</p>

## Azure

### Step 1: Set up VNet peering between Hopsworks and Databricks

VNet peering between the Hopsworks and the Databricks virtual network is required to be able to connect
to the Feature Store from Databricks.

In the Azure portal, go to Azure Databricks and go to *Virtual Network Peering*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-7.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-7.png" alt="Azure Databricks">
    </a>
    <figcaption>Azure Databricks</figcaption>
  </figure>
</p>

Select *Add Peering*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-8.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-8.png" alt="Add peering">
    </a>
    <figcaption>Add peering</figcaption>
  </figure>
</p>

Name the peering and select the virtual network used by your Hopsworks cluster. The virtual network
is shown in the cluster details on Hopsworks.ai (see the next picture). Ensure to press the copy button
on the bottom of the page and save the value somewhere. Press *Add* and create the peering:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-10.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-10.png" alt="Configure peering">
    </a>
    <figcaption>Configure peering</figcaption>
  </figure>
</p>

The virtual network used by your cluster is shown under *Details*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-9.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-9.png" alt="Check the Hopsworks virtual network">
    </a>
    <figcaption>Check the Hopsworks virtual network</figcaption>
  </figure>
</p>

The peering connection should now be listed as initiated:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-11.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-11.png" alt="Peering connection initiated">
    </a>
    <figcaption>Peering connection initiated</figcaption>
  </figure>
</p>

On the Azure portal, go to *Virtual networks* and search for the virtual network used by your
Hopsworks cluster:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-12.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-12.png" alt="Virtual networks">
    </a>
    <figcaption>Virtual networks</figcaption>
  </figure>
</p>

Open the network and select *Peerings*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-13.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-13.png" alt="Select peerings">
    </a>
    <figcaption>Select peerings</figcaption>
  </figure>
</p>

Choose to add a peering connection:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-14.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-14.png" alt="Add a peering connection">
    </a>
    <figcaption>Add a peering connection</figcaption>
  </figure>
</p>

Name the peering connection and select *I know my resource ID*. Paste the string copied when creating
the peering from Databricks Azure. If you haven't copied that string, then manually select the virtual
network used by Databricks and press *OK* to create the peering:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-15.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-15.png" alt="Configure peering">
    </a>
    <figcaption>Configure peering</figcaption>
  </figure>
</p>

The peering should now be *Updating*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-16.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-16.png" alt="Cloud account settings">
    </a>
    <figcaption>Cloud account settings</figcaption>
  </figure>
</p>

Wait for the peering to show up as *Connected*. There should now be bi-directional network connectivity between the Feature Store and Databricks:

<p align="center">
  <figure>
    <a  href="../../../assets/images/databricks/azure/azure-databricks-17.png">
      <img src="../../../assets/images/databricks/azure/azure-databricks-17.png" alt="Cloud account settings">
    </a>
    <figcaption>Cloud account settings</figcaption>
  </figure>
</p>

### Step 2: Configure the Network Security Group

The *Network Security Group* of the Feature Store on Azure needs to be configured to allow traffic from your Databricks clusters to be able to connect to the Feature Store.

Ensure that ports *443*, *9083*, *9085*, *8020* and *50010* are reachable from the Databricks cluster *Network Security Group*.

!!! note "Hopsworks.ai"
    If you deployed your Hopsworks Feature Store instance with Hopsworks.ai, it suffices to enable [outside access of the Feature Store and Online Feature Store services](../../../hopsworksai/azure/getting_started/#step-6-outside-access-to-the-feature-store).

## Next Steps

Continue with the [Hopsworks API key guide](api_key.md) to setup access to a Hopsworks API key from the Databricks Cluster, in order to be able to use the Hopsworks Feature Store.
