# Getting started with Hopsworks.ai (Azure)

Hopsworks.ai is our managed platform for running Hopsworks and the Feature Store
in the cloud. It integrates seamlessly with third party platforms such as Databricks,
SageMaker and KubeFlow. This guide shows how to set up Hopsworks.ai with your organization's Azure account.

## Step 1: Connecting your Azure account

Hopsworks.ai deploys Hopsworks clusters to your Azure account. To enable this, you have to
create a service principal and a custom role for Hopsworks.ai granting access
to either a subscription or resource group.

### Step 1.1: Creating a service principal for Hopsworks.ai

On Hopsworks.ai, go to *Settings/Cloud Accounts* and choose to *Configure* Azure:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-0.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-0.png" alt="Cloud account settings">
    </a>
    <figcaption>Cloud account settings</figcaption>
  </figure>
</p>

Select *Add subscription key*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-0.1.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-0.1.png" alt="Add subscription keys">
    </a>
    <figcaption>Add subscription keys</figcaption>
  </figure>
</p>

The Azure account configuration will show you the required steps and permissions.
Ensure that you have the Azure CLI installed [Install the Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli)
and are logged in [Sign in with Azure CLI](https://docs.microsoft.com/en-us/cli/azure/authenticate-azure-cli).

Copy the Azure CLI command from the first step and open a terminal:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-1.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-1.png" alt="Connect your Azure Account">
    </a>
    <figcaption>Connect your Azure Account</figcaption>
  </figure>
</p>

Paste the command into the terminal and execute it:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-2.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-2.png" alt="Add service principal">
    </a>
    <figcaption>Add service principal</figcaption>
  </figure>
</p>

At this point, you might get the following error message.
This means that your Azure user does not have sufficient permissions to add the service principal.
In this case, please ask your Azure administrator to add it for you or give you the required permissions.

```bash
$ az ad sp create --id d4abcc44-2c40-40bd-9bba-986df591c28f
```

!!! note
    When using this permission, the backing application of the service principal being created must in the local tenant.

### Step 1.2: Creating a custom role for Hopsworks.ai

Proceed to the Azure Portal and open either a *Subscription* or *Resource Group* that you want to use for Hopsworks.ai.
Select *Add* and choose *Add custom role*. Granting access to a *Subscription* will grant access to all *Resource Groups*
in that *Subscription*. If you are uncertain if that is what you want, then start with a *Resource Group*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-3.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-3.png" alt="Add custom role">
    </a>
    <figcaption>Add custom role</figcaption>
  </figure>
</p>

Name the role and proceed to *Assignable scopes*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-4.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-4.png" alt="Name custom role">
    </a>
    <figcaption>Name custom role</figcaption>
  </figure>
</p>

Ensure the scope is set to the *Subscription* or *Resource Group* you want to use.
You can change it here if required. Proceed to the JSON tab:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-5.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-5.png" alt="Review assignable scope">
    </a>
    <figcaption>Review assignable scope</figcaption>
  </figure>
</p>

Select *Edit* and replace the *actions* part of the JSON with the one from Hopsworks.ai Azure account configuration workflow:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-5.1.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-5.1.png" alt="Hopsworks.ai permission list">
    </a>
    <figcaption>Hopsworks.ai permission list</figcaption>
  </figure>
</p>

Press *Save*, proceed to *Review + create* and create the role:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-6.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-6.png" alt="Update permission JSON">
    </a>
    <figcaption>Update permission JSON</figcaption>
  </figure>
</p>

### Step 1.3: Assigning the custom role to Hopsworks.ai

Back in the *Subscription* or *Resource Group* overview, select *Add* and choose *Add role assignment*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-7.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-7.png" alt="Add role assignment">
    </a>
    <figcaption>Add role assignment</figcaption>
  </figure>
</p>

Choose the custom role you just created, select *User, group, or service principal* to *Assign access to*
and select the *hopsworks.ai* service principal. Press *Save*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-8.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-8.png" alt="Configure Hopsworks.ai as role assignment">
    </a>
    <figcaption>Configure Hopsworks.ai as role assignment</figcaption>
  </figure>
</p>

Go back to the Hopsworks.ai Azure account configuration workflow and proceed to the next step. Copy the CLI command shown:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-9.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-9.png" alt="Configure subscription and tenant id">
    </a>
    <figcaption>Configure subscription and tenant id</figcaption>
  </figure>
</p>

Paste the CLI command into your terminal and execute it. Note that you might have multiple entries listed here.
If so, ensure that you pick the subscription that you want to use.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-10.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-10.png" alt="Show subscription and tenant id">
    </a>
    <figcaption>Show subscription and tenant id</figcaption>
  </figure>
</p>

Copy the value of *id* and paste it into the *Subscription id*
field on Hopsworks.ai. Go back to the terminal and copy the value of *tenantId*. Ensure to NOT use the *tenantId* under *managedByTenants*.
Paste the value into the *Tenant ID* field on Hopsworks.ai and press *Finish*.

Congratulations, you have successfully connected you Azure account to Hopsworks.ai.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-10.1.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-10.1.png" alt="Store subscription and tenant id">
    </a>
    <figcaption>Store subscription and tenant id</figcaption>
  </figure>
</p>

## Step 2: Creating and configuring a storage

The Hopsworks clusters deployed by hopsworks.ai store their data in a container in your Azure account.
To enable this you need to create a storage account and a User Assigned Managed Identity to give the Hopsworks cluster access to the storage.

### Step 2.1: Creating a User Assigned Managed Identity

Proceed to the Azure Portal and open the Resource Group that you want to use for Hopsworks.ai. Click on *Add*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/add-to-resource-group.png">
      <img src="../../../assets/images/hopsworksai/azure/add-to-resource-group.png" alt="Add to resource group">
    </a>
    <figcaption>Add to resource group</figcaption>
  </figure>
</p>

Search for *User Assigned Managed Identity* and click on it.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/search-user-assigned-identity.png">
      <img src="../../../assets/images/hopsworksai/azure/search-user-assigned-identity.png" alt="Search User Assigned Managed Identity">
    </a>
    <figcaption>Search User Assigned Managed Identity</figcaption>
  </figure>
</p>

Click on *Create*. Then, select the Location you want to use and name the identity. Click on *Review + create*. Finally click on *Create*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/create-user-assigned-identity.png">
      <img src="../../../assets/images/hopsworksai/azure/create-user-assigned-identity.png" alt="Create a User Assigned Managed Identity">
    </a>
    <figcaption>Create a User Assigned Managed Identity</figcaption>
  </figure>
</p>

### Step 2.2: Creating a Storage account

Proceed to the Azure Portal and open the Resource Group that you want to use for Hopsworks.ai. Click on *Add*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/add-to-resource-group.png">
      <img src="../../../assets/images/hopsworksai/azure/add-to-resource-group.png" alt="Add to resource group">
    </a>
    <figcaption>Add to resource group</figcaption>
  </figure>
</p>

Search for *Storage account* and click on it.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/search-storage-account.png">
      <img src="../../../assets/images/hopsworksai/azure/search-storage-account.png" alt="Search Storage Account Identity">
    </a>
    <figcaption>Search Storage Account Identity</figcaption>
  </figure>
</p>

Click on *Create*, name your storage account, select the Location you want to use and click on *Review + create*. Finally click on *Create*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/create-storage-account.png">
      <img src="../../../assets/images/hopsworksai/azure/create-storage-account.png" alt="Create a Storage Account">
    </a>
    <figcaption>Create a Storage Account</figcaption>
  </figure>
</p>

### Step 2.3: Give the Managed Identity access to the storage

Proceed to the Storage Account you just created and click on *Access Control (IAM)* (1). Click on *Add* (2), then click on *Add role assignment* (3).
In *Role* select *Storage Blob Data Owner* (4). In *Assign access to* select *User assigned managed identity* (5). Select the identity you created in step 2.1 (6).
Click on *Save* (7).

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/add-role-to-storage.png">
      <img src="../../../assets/images/hopsworksai/azure/add-role-to-storage.png" alt="Add role assignment to storage">
    </a>
    <figcaption>Add role assignment to storage</figcaption>
  </figure>
</p>

## Step 3: Deploying a Hopsworks cluster

In Hopsworks.ai, select *Create cluster*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create_instance.png">
      <img src="../../../assets/images/hopsworksai/create_instance.png" alt="Create a Hopsworks cluster">
    </a>
    <figcaption>Create a Hopsworks cluster</figcaption>
  </figure>
</p>

Select the *Location* in which you want your cluster to run (1), name your cluster (2) and select the *Resource Group* (3) in which you created your *storage account* and *user assigned managed identity* (see above).

Select the *Instance type* (4) and *Local storage* (5) size for the cluster *Head node*.

Select the number of workers you want to start the cluster with (6).
Select the *Instance type* (7) and *Local storage* size (8) for the *worker nodes*.

!!! note
    It is possible to add or remove workers once the cluster is running.

Enter the name of the *storage account* (9) you created above in *Azure Storage account name* and name the container in which the data wil be stored in *Azure Container name* (10).

!!! note
    You can choose to use a container already existing in you *storage account* by using the name of this container, but you need to fist make sure that this container is empty.

Press *Next* (11):

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-11.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-11.png" alt="General configuration">
    </a>
    <figcaption>General configuration</figcaption>
  </figure>
</p>

Select the *SSH key* that you want to use to access cluster instances:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-12.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-12.png" alt="Choose SSH key">
    </a>
    <figcaption>Choose SSH key</figcaption>
  </figure>
</p>

Select the *User assigned managed identity* that you created above:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-identity.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-identity.png" alt="Choose the User assigned managed identity">
    </a>
    <figcaption>Choose the User assigned managed identity</figcaption>
  </figure>
</p>

Select the *Virtual Network* or choose to automatically create a new one:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-13.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-13.png" alt="Choose virtual network">
    </a>
    <figcaption>Choose virtual network</figcaption>
  </figure>
</p>

Select the *Subnet* or choose to automatically create a new one:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-14.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-14.png" alt="Choose subnet">
    </a>
    <figcaption>Choose subnet</figcaption>
  </figure>
</p>

Select the *Security group* or choose to automatically create a new one:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-15.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-15.png" alt="Choose security group">
    </a>
    <figcaption>Choose security group</figcaption>
  </figure>
</p>

Choose the user management you want. Select *Managed* to manage users via Hopsworks.ai, *LDAP* to integrate with your organization's LDAP/ActiveDirectory server or *Disabled* to manage users manually from within Hopsworks:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-16.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-16.png" alt="Choose user management type">
    </a>
    <figcaption>Choose user management type</figcaption>
  </figure>
</p>

Review all information and select *Create*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-17.png">
      <img src="../../../assets/images/hopsworksai/azure/connect-azure-17.png" alt="Review cluster information">
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

As soon as the cluster has started, you will be able to log in to your new Hopsworks cluster with the username and password provided. You will also able to stop, restart or terminate the cluster.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/running.png">
      <img src="../../../assets/images/hopsworksai/running.png" alt="Running Hopsworks cluster">
    </a>
    <figcaption>Running Hopsworks cluster</figcaption>
  </figure>
</p>

## Step 3: Outside Access to the Feature Store

By default, only the Hopsworks REST API (and UI) is accessible by clients on external networks, like the Internet.
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
