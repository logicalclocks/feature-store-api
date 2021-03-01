# Integration with Azure AKS and ACR

This guide shows how to create a cluster in hopsworks.ai with integrated support for Azure Kubernetes Service (AKS) and Azure Container Registry (ACR). This enables Hopsworks to launch Python jobs, Jupyter servers, and serve models on top of AKS.

Hopsworks AKS and ACR integration have four requirements:

1. A virutal network with access to AKS pods and the AKS API servers 
2. One Azure container registry configured in your account
3. One AKS cluster
4. Permissions to the ACR and AKS attached to a user-managed identity

This guide provides an example setup with a private AKS cluster and public ACR.
!!! note
A public AKS cluster means the kubernetes API server is accessible outside the virtual network it is deployed in. Similarly, a public ACR is accessible through the internet.


## User assigned managed identity (managed identity)

!!! note
A user assigned managed identity (managed identity) can be created at the subscription level or to a specific resource group in a subscription. The managed identity is attached to the virtual machines that run inside your subscription (or resource group). Hence, the permissions only apply to services that run within your subscription (or resource group). 

AKS and ACR integration requires some additional permissions to be attached to the managed identity used by the Hopsworks cluster. 

To setup the managed identity, go to the resource group where you will add the managed identity - this should be the same resource group as you deploy Hopsworks in. In your selected resource group, click on the Access Control (IAM) menu item, and then click on add button. In the search dialog, enter "user assigned managed identity", and then select a name for the managed identity, review it and create it.

### Add role assignment at subscription level

Next go to the created managed identity in order to create role assignments. Go to azure role assignments and add the following roles:

* AcrPull
* AcrPush
* AcrDelete
* Azure Kubernetes Service Contributor Role

The roles can have either scope of your subscription or the specific resource group you are planning to use. A scope of the resource group will restrict the permissions of the roles to the chosen resource group.

!!! warning
You will also need to attach the Storage Blob Data Owner role to the managed identity, see [Creating and configuring a storage](getting_started.md#step-2-creating-and-configuring-a-storage)

Once finished the role assignments should look similar to the picture below. Note that hopsworks-stage is used as the resource group throughout the example setup.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-permissions.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-permissions.png" alt="AKS permissions">
    </a>
    <figcaption>AKS permissions</figcaption>
  </figure>
</p>

## Private AKS cluster and public ACR

This guide will step through setting up a private AKS cluster and a public ACR. 

### Step 1 Create an AKS cluster
Go to Kubernetes services in the azure portal and click add. Place the Kubernetes cluster in the same resource group as the Hopsworks cluster and choose a name for the Kubernetes cluster.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-base.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-base.png" alt="AKS general configuration">
    </a>
    <figcaption>AKS general configuration</figcaption>
  </figure>
</p>

Next click the Authentication tab and verify the settings are identical to the picture below.
!!! note
Currently AKS is only supported through managed identities. Contact the Logical Clocks sales team if you have a self-managed Kubernetes cluster.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-base.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-base.png" alt="AKS authentication configuration">
    </a>
    <figcaption>AKS authencation configuration</figcaption>
  </figure>
</p>

Next go to the networking tab and check Azure CNI. The portal will automatically fill in the IP address ranges for the kubernetes virtual network. Take note of the virtual network name that is created, in this example the virtual network name was hopsworksstagevnet154. Lastly, check the private cluster option. 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-base.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-base.png" alt="AKS network configuration">
    </a>
    <figcaption>AKS network configuration</figcaption>
  </figure>
</p>

Next go to integrations. Under container registry click Create new. 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-acr-create.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-acr-create.png" alt="AKS create ACR">
    </a>
    <figcaption>AKS create ACR</figcaption>
  </figure>
</p>

Choose a name for the registry and check the SKU to be premium. Then press OK. 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-acr.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-acr.png" alt="AKS ACR configuration">
    </a>
    <figcaption>ACR configuration</figcaption>
  </figure>
</p>

Next press review and create, then create the cluster.

Because the kubernetes API service is private the Hopsworks cluster must be able to reach it over a private network. There are two options to integrate with a private AKS cluster. The first option is to put the Hopsworks cluster in a pre-defined virtual network with peering setup to the kubernetes network. The second option is to create a subnet inside the kubernetes virtual network where the Hopsworks cluster will be placed.

#### Step 1 option a Peering setup

In order to establish virtual peering between the kubernetes cluster and Hopsworks, you need to select or create a virtual network for Hopsworks. Go to virtual networks and press add.
Choose a name for the new virtual network and select the same resource group you are planning to use for your Hopsworks cluster.

Next, go to the IP Addresses tab. Create an address space which does not overlap with the address space in the kubernetes network. In the previous example, the automatically created kubernetes network used the address space 10.0.0.0/8. Hence, the address space 172.18.0.0/16 can safely be used.

Next click review and create, then create.

Next go to the created virtual network and go to the peerings tab. Then click add. 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-peering.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-peering.png" alt="Virtual network peering">
    </a>
    <figcaption>Virtual network peering</figcaption>
  </figure>
</p>

Choose a name for the peering link. Check the Traffic to remote virtual network as allow, and Traffic forwarded from remote virtual network to block.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-peering1.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-peering1.png" alt="Virtual network peering">
    </a>
    <figcaption>Virtual network peering configuration</figcaption>
  </figure>
</p>

For the virtual network select the virtual network which was created by AKS, in our example this was hopsworksstagevnet154. Then press add.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-peering2.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-peering2.png" alt="Virtual network peering">
    </a>
    <figcaption>Virtual network peering configuration continuation</figcaption>
  </figure>
</p>

The last step is to setup a DNS private link in order to be able to use DNS resolution for the kubernetes API servers. Go to resource groups in the Azure portal and find the resource group of the kubernetes cluster. This will be in the form of MC_hopsworks-stage_<cluster_name>_<region> in this example it was MC_hopsworks-stage_hopsworks-aks_northeurope. Open the resource group and click on the DNS zone.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-private-dns.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-private-dns.png" alt="Virtual network peering">
    </a>
    <figcaption>Private DNS link setup</figcaption>
  </figure>
</p>

In the left plane there is a tab called Virtual network links, click on the tab. Next press add.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-vnet-link.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-vnet-link.png" alt="">
    </a>
    <figcaption>Private DNS link configuration</figcaption>
  </figure>
</p>

Choose a name for the private link and select the virtual network you will use for the Hopsworks cluster, then press OK.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-vnet-link-config.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-vnet-link-config.png" alt="">
    </a>
    <figcaption>Private DNS link configuration</figcaption>
  </figure>
</p>

The setup is now finalized and you can create the Hopsworks cluster.

#### Step 1 option b Subnet in AKS network

With this setup the Hopsworks cluster will reside in the same virtual network as the AKS cluster. The difference is that a new subnet in the virtual network will be used for the Hopsworks cluster.

To setup the subnet, first go to the virtual network that was created by AKS. In our example, this was hopsworksstagevnet154. Next go to the subnets tab.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-subnet.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-subnet.png" alt="">
    </a>
    <figcaption>AKS subnet setup</figcaption>
  </figure>
</p>

Press + Subnet. Choose a name for the subnet, for example, "hopsworks" and an IP range that does not overlap with the kubernetes network. Then save.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-subnet-config.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-subnet-config.png" alt="">
    </a>
    <figcaption>AKS subnet setup</figcaption>
  </figure>
</p>

### Create the Hopsworks cluster

This step assumes you are creating your Hopsworks cluster using hopsworks.ai. The AKS configuration can be set under the Managed containers tab. Select the Use Azure AKS and Azure ACR as enabled. Two new fields with the name of the container registry you created and the AKS cluster name will pop up. In the previous example we created an ACR with name hopsaksregistry and an AKS cluster with name hopsaks-cluster. Hence, the configuration should look similar to the picture below

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/aks-hops-config.png">
      <img src="../../../assets/images/hopsworksai/azure/aks-hops-config.png" alt="">
    </a>
    <figcaption>Hopsworks AKS configuration</figcaption>
  </figure>
</p>

In the virtual network tab you have to select the either the virtual network you created for the peering setup or the kubernetes virtual network depending on which approach you choose. Under the subnet tab you have to choose the default subnet if you choose the peering approach, or the subnet you created if you choose to create a new subnet inside the AKS virtual network.

