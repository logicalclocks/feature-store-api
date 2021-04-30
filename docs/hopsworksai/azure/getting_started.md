# Getting started with Hopsworks.ai (Azure)

Hopsworks.ai is our managed platform for running Hopsworks and the Feature Store
in the cloud. It integrates seamlessly with third party platforms such as Databricks,
SageMaker and KubeFlow. This guide shows how to set up Hopsworks.ai with your organization's Azure account.

## Step 1: Connecting your Azure account

Hopsworks.ai deploys Hopsworks clusters to your Azure account. To enable this, you have to
create a service principal and a custom role for Hopsworks.ai granting access
to either a subscription or resource group.

<p align="center">
  <iframe
    title="Azure information video"
    style="width:700px; height: 370px;"
    src="https://www.youtube.com/embed/Pfx2b3UTt88"
    frameBorder="0"
    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
    allowFullScreen
  >
  </iframe>
</p>

### Step 1.0: Prerequisite

For Hopsworks.ai to deploy a cluster the following resource providers need to be registered on your Azure subscription.
You can verify that they are registered by going to your subscription in the Azure portal and click on *Resource providers*.
If one of the resource providers is not registered select it and click on *Register*.

```json
    Microsoft.Network
    Microsoft.Compute
    Microsoft.Storage
    Microsoft.ManagedIdentity
```

### Step 1.1: Creating a service principal for Hopsworks.ai

On Hopsworks.ai, go to *Settings/Cloud Accounts* and choose to *Configure* Azure:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-0.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-0.png" alt="Cloud account settings">
    </a>
    <figcaption>Cloud account settings</figcaption>
  </figure>
</p>

Select *Add subscription key*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-0.1.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-0.1.png" alt="Add subscription keys">
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
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-1.png" alt="Connect your Azure Account">
    </a>
    <figcaption>Connect your Azure Account</figcaption>
  </figure>
</p>

Paste the command into the terminal and execute it:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-2.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-2.png" alt="Add service principal">
    </a>
    <figcaption>Add service principal</figcaption>
  </figure>
</p>

At this point, you might get the following error message.
This means that your Azure user does not have sufficient permissions to add the service principal.
In this case, please ask your Azure administrator to add it for you or give you the required permissions.

!!! error

    ```bash
    az ad sp create --id d4abcc44-2c40-40bd-9bba-986df591c28f
    ```

    When using this permission, the backing application of the service principal being created must in the local tenant.


### Step 1.2: Creating a custom role for Hopsworks.ai

Proceed to the Azure Portal and open either a *Subscription* or *Resource Group* that you want to use for Hopsworks.ai. Click on *Access control (IAM)*
Select *Add* and choose *Add custom role*. 

!!! note
    Granting access to a *Subscription* will grant access to all *Resource Groups* 
    in that *Subscription*. If you are uncertain if that is what you want, then start with a *Resource Group*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-3.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-3.png" alt="Add custom role">
    </a>
    <figcaption>Add custom role</figcaption>
  </figure>
</p>

Name the role and proceed to *Assignable scopes*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-4.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-4.png" alt="Name custom role">
    </a>
    <figcaption>Name custom role</figcaption>
  </figure>
</p>

Ensure the scope is set to the *Subscription* or *Resource Group* you want to use.
You can change it here if required. Proceed to the JSON tab:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-5.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-5.png" alt="Review assignable scope">
    </a>
    <figcaption>Review assignable scope</figcaption>
  </figure>
</p>

Select *Edit* and replace the *actions* part of the JSON with the one from Hopsworks.ai Azure account configuration workflow:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-5.1.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-5.1.png" alt="Hopsworks.ai permission list">
    </a>
    <figcaption>Hopsworks.ai permission list</figcaption>
  </figure>
</p>

!!! note
    If the access rights provided by Hopsworks.ai Azure account configuration workflow are too permissive, you can go to [Limiting Azure permissions](./restrictive_permissions.md) for more details on how to limit the permissions.

Press *Save*, proceed to *Review + create* and create the role:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-6.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-6.png" alt="Update permission JSON">
    </a>
    <figcaption>Update permission JSON</figcaption>
  </figure>
</p>

### Step 1.3: Assigning the custom role to Hopsworks.ai

Back in the *Subscription* or *Resource Group*, in *Access control (IAM)*, select *Add* and choose *Add role assignment*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-7.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-7.png" alt="Add role assignment">
    </a>
    <figcaption>Add role assignment</figcaption>
  </figure>
</p>

Choose the custom role you just created, select *User, group, or service principal* to *Assign access to*
and select the *hopsworks.ai* service principal. Press *Save*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-8.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-8.png" alt="Configure Hopsworks.ai as role assignment">
    </a>
    <figcaption>Configure Hopsworks.ai as role assignment</figcaption>
  </figure>
</p>

Go back to the Hopsworks.ai Azure account configuration workflow and proceed to the next step. Copy the CLI command shown:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-9.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-9.png" alt="Configure subscription and tenant id">
    </a>
    <figcaption>Configure subscription and tenant id</figcaption>
  </figure>
</p>

Paste the CLI command into your terminal and execute it. Note that you might have multiple entries listed here.
If so, ensure that you pick the subscription that you want to use.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-10.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-10.png" alt="Show subscription and tenant id">
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
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-10.1.png" alt="Store subscription and tenant id">
    </a>
    <figcaption>Store subscription and tenant id</figcaption>
  </figure>
</p>

## Step 2: Creating and configuring a storage

The Hopsworks clusters deployed by hopsworks.ai store their data in a container in your Azure account. To enable this you need to perform the following operations

- Create a restrictive role to limit access to the storage account
- Create a User Assigned Managed Identity
- Create a storage account and give Hopsworks clusters access to the storage using the restrictive role

### Step 2.1: Creating a Restrictive Role for Accessing Storage 

Similarly to [Step 1.2](#step-12-creating-a-custom-role-for-hopsworksai) create a new role named `Hopsworks Storage Role`. Add the following permissions to the role

```json
"permissions": [
    {
        "actions": [
            "Microsoft.Storage/storageAccounts/blobServices/containers/write",
            "Microsoft.Storage/storageAccounts/blobServices/containers/read",
            "Microsoft.Storage/storageAccounts/blobServices/write",
            "Microsoft.Storage/storageAccounts/blobServices/read"
        ],
        "notActions": [],
        "dataActions": [
            "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/delete",
            "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read",
            "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/move/action",
            "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write"
        ],
        "notDataActions": []
    }
]
```

!!! note
    Some of these permissions can be removed at the cost of Hopsworks features, see [Limiting Azure permissions](./restrictive_permissions.md) for more details.

### Step 2.2: Creating a User Assigned Managed Identity

Proceed to the Azure Portal and open the Resource Group that you want to use for Hopsworks.ai. Click on *Add* then *Marketplace*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/add-to-resource-group.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/add-to-resource-group.png" alt="Add to resource group">
    </a>
    <figcaption>Add to resource group</figcaption>
  </figure>
</p>

Search for *User Assigned Managed Identity* and click on it.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/search-user-assigned-identity.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/search-user-assigned-identity.png" alt="Search User Assigned Managed Identity">
    </a>
    <figcaption>Search User Assigned Managed Identity</figcaption>
  </figure>
</p>

Click on *Create*. Then, select the Location you want to use and name the identity. Click on *Review + create*. Finally click on *Create*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/create-user-assigned-identity.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/create-user-assigned-identity.png" alt="Create a User Assigned Managed Identity">
    </a>
    <figcaption>Create a User Assigned Managed Identity</figcaption>
  </figure>
</p>

### Step 2.3: Creating a Storage account

Proceed to the Azure Portal and open the Resource Group that you want to use for Hopsworks.ai. Click on *Add* then *Marketplace*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/add-to-resource-group.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/add-to-resource-group.png" alt="Add to resource group">
    </a>
    <figcaption>Add to resource group</figcaption>
  </figure>
</p>

Search for *Storage account* and click on it.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/search-storage-account.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/search-storage-account.png" alt="Search Storage Account Identity">
    </a>
    <figcaption>Search Storage Account Identity</figcaption>
  </figure>
</p>

Click on *Create*, name your storage account, select the Location you want to use and click on *Review + create*. Finally click on *Create*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/create-storage-account.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/create-storage-account.png" alt="Create a Storage Account">
    </a>
    <figcaption>Create a Storage Account</figcaption>
  </figure>
</p>

### Step 2.4: Give the Managed Identity access to the storage

Proceed to the Storage Account you just created and click on *Access Control (IAM)* (1). Click on *Add* (2), then click on *Add role assignment* (3).
In *Role* select *Hopsworks Storage Role* (4). In *Assign access to* select *User assigned managed identity* (5). Select the identity you created in step 2.1 (6).
Click on *Save* (7).

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/add-role-to-storage.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/add-role-to-storage.png" alt="Add role assignment to storage">
    </a>
    <figcaption>Add role assignment to storage</figcaption>
  </figure>
</p>

## Step 3: Adding a ssh key to your resource group

When deploying clusters, Hopsworks.ai installs a ssh key on the cluster's instances so that you can access them if necessary. For this purpose you need to add a ssh key to your resource group.

Proceed to the Azure Portal and open the Resource Group that you want to use for Hopsworks.ai. Click on *Add* then *Marketplace*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/add-to-resource-group.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/add-to-resource-group.png" alt="Add to resource group">
    </a>
    <figcaption>Add to resource group</figcaption>
  </figure>
</p>

Search for *SSH Key* and click on it. Click on Create. Then, name your key pair and choose between *Generate a new key pair* and *Upload existing public key*. Click on *Review + create*. Finally click on *Create*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/create-ssh-key.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/create-ssh-key.png" alt="Create a SSH key">
    </a>
    <figcaption>Add to resource group</figcaption>
  </figure>
</p>

## Step 4: Deploying a Hopsworks cluster

In Hopsworks.ai, select *Create cluster*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-instance.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/create-instance.png" alt="Create a Hopsworks cluster">
    </a>
    <figcaption>Create a Hopsworks cluster</figcaption>
  </figure>
</p>

Select the *Resource Group* (1) in which you created your *storage account* and *user assigned managed identity* (see above).

!!! note
    If the *Resource Group* does not appear in the drop-down, make sure that you properly [created and set the custom role](#step-12-creating-a-custom-role-for-hopsworksai) for this resource group.

Name your cluster (2). Your cluster will be deployed in the *Location* of your *Resource Group* (3).

Select the *Instance type* (4) and *Local storage* (5) size for the cluster *Head node*.

!!! note
    It is possible to add or remove workers once the cluster is running.

Select the *storage account* (6) you created above in *Azure Storage account name*. The name of the container in which the data will be stored is displayed in *Azure Container name* (7), you can modify it if needed.

!!! note
    You can choose to use a container already existing in your *storage account* by using the name of this container, but you need to first make sure that this container is empty.

Press *Next*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/create-instance-general.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/create-instance-general.png" alt="General configuration">
    </a>
    <figcaption>General configuration</figcaption>
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
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-12.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-12.png" alt="Choose SSH key">
    </a>
    <figcaption>Choose SSH key</figcaption>
  </figure>
</p>

Select the *User assigned managed identity* that you created above and click on *Review and Create*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-identity.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-identity.png" alt="Choose the User assigned managed identity">
    </a>
    <figcaption>Choose the User assigned managed identity</figcaption>
  </figure>
</p>

!!! note
    If you are an enterprise user you will have one more step before being able to click on *Review and Create*. In this step, you will be asked to set the backups retention policy. More details about this step [here](./cluster_creation.md#step-6-set-the-backup-retention-policy)

Review all information and select *Create*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/connect-azure-17.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/azure/connect-azure-17.png" alt="Review cluster information">
    </a>
    <figcaption>Review cluster information</figcaption>
  </figure>
</p>

!!! note
    We skipped cluster creation steps that are not mandatory. You can find more details about these steps [here](./cluster_creation.md)

The cluster will start. This will take a few minutes:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/booting.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/booting.png" alt="Booting Hopsworks cluster">
    </a>
    <figcaption>Booting Hopsworks cluster</figcaption>
  </figure>
</p>

As soon as the cluster has started, you will be able to log in to your new Hopsworks cluster with the username and password provided. You will also be able to stop, restart or terminate the cluster.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/running.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/running.png" alt="Running Hopsworks cluster">
    </a>
    <figcaption>Running Hopsworks cluster</figcaption>
  </figure>
</p>

## Step 5: Outside Access to the Feature Store

By default, only the Hopsworks REST API (and UI) is accessible by clients on external networks, like the Internet.
To integrate with external platforms and access APIs for services such as the Feature Store, you have to open the service's ports.

Open ports by going to *Services* tab, selecting a service and pressing *Update*. This will update the *Security Group* attached to the Hopsworks cluster to allow incoming traffic on the relevant ports.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/open-ports.png">
      <img style="border: 1px solid #000;width:700px" src="../../../assets/images/hopsworksai/open-ports.png" alt="Outside Access to the Feature Store">
    </a>
    <figcaption>Outside Access to the Feature Store</figcaption>
  </figure>
</p>

## Step 6: Next steps

Check out our other guides for how to get started with Hopsworks and the Feature Store:

* Get started with the [Hopsworks Feature Store](../../quickstart.md)
* Get started with Machine Learning on Hopsworks: [HopsML](https://hopsworks.readthedocs.io/en/stable/hopsml/index.html#hops-ml)
* Get started with Hopsworks: [User Guide](https://hopsworks.readthedocs.io/en/stable/user_guide/user_guide.html#userguide)
* Code examples and notebooks: [hops-examples](https://github.com/logicalclocks/hops-examples)
