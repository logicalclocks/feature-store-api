# Upgrade existing clusters on Hopsworks.ai (Azure)
This guide shows you how to upgrade your existing Hopsworks cluster to a newer version of Hopsworks. First, a notification will appear on the top of your cluster when a new version is available as shown in the figure below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-notification-running.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-notification-running.png" alt="New version notification">
    </a>
    <figcaption>A new Hopsworks version is available</figcaption>
  </figure>
</p>

## Step 1: Stop your cluster 

You need to **Stop** your cluster to start the upgrade process. Once your cluster is stopped, the *Upgrade* button will appear as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-notification-stopped.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-notification-stopped.png" alt="New version notification">
    </a>
    <figcaption>A new Hopsworks version is available</figcaption>
  </figure>
</p>

## Step 2: Add upgrade permissions to your user assigned managed identity

We require extra permissions to be added to the user assigned managed identity attached to your cluster to proceed with the upgrade. First to get the name of your user assigned managed identity and the resource group of your cluster, click on the *Details* tab as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-managed-identity-details.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-managed-identity-details.png" alt="Azure details tab">
    </a>
    <figcaption>Get the resource group name (1) and the user assigned managed identity (2) of your cluster</figcaption>
  </figure>
</p>

### Step 2.1: Add custom role for upgrade permissions 

Once you get the names of the resource group and user-assigned managed identity, follow the same steps as in [getting started to add a custom role](../getting_started/#step-12-creating-a-custom-role-for-hopsworksai). First, navigate to [Azure portal](https://portal.azure.com/#home), then click on *Resource groups* and then search for your resource group and click on it. Go to the *Access control (IAM)* tab, select *Add*, and click on *Add custom role* 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade.png" alt="Azure add custom role">
    </a>
    <figcaption>Add a custom role for upgrade</figcaption>
  </figure>
</p>

Name the custom role and then click on next till you reach the *JSON* tab.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade-1.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade-1.png" alt="Azure add custom role">
    </a>
    <figcaption>Name the custom role for upgrade</figcaption>
  </figure>
</p>

Once you reach the *JSON* tab, click on *Edit* to edit the role permissions:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade-2.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade-2.png" alt="Azure add custom role">
    </a>
    <figcaption>Edit the JSON permissions for the custom role for upgrade</figcaption>
  </figure>
</p>

Once you have clicked on *Edit*, replace the *permissions* array with the following snippet:

```json
"permissions": [
    {
        "actions": [
            "Microsoft.Compute/virtualMachines/read",
            "Microsoft.Compute/virtualMachines/write",
            "Microsoft.Compute/disks/read",
            "Microsoft.Compute/disks/write"
        ],
        "notActions": [],
        "dataActions": [],
        "notDataActions": []
    }
]
```

Then, click on *Save* to save the updated permissions

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade-3.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade-3.png" alt="Azure add custom role">
    </a>
    <figcaption>Save permissions for the custom role for upgrade</figcaption>
  </figure>
</p>

Click on *Review and create* and then click on *Create* to create the custom role:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade-4.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-portal-add-custom-upgrade-4.png" alt="Azure add custom role">
    </a>
    <figcaption>Save permissions for the custom role for upgrade</figcaption>
  </figure>
</p>

### Step 2.2: Assign the custom role to your user-assigned managed identity

Navigate back to the your Resource group home page at [Azure portal](https://portal.azure.com/#home), click on *Add* and then click on *Add role assignment*

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-portal-assign-upgrade-role-to-managed-identity.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-portal-assign-upgrade-role-to-managed-identity.png" alt="Azure add custom role">
    </a>
    <figcaption>Assign upgrade role to your user assigned managed identity</figcaption>
  </figure>
</p>

(1) choose the upgrade role that you have just created in [Step 2.1](#step-21-add-custom-role-for-upgrade-permissions), (2) choose *User Assigned Managed Identity*, (3) search for the user assigned managed identity attached to your cluster and select it. Finally, (4) click on *Save* to save the role assignment.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-portal-assign-upgrade-role-to-managed-identity-1.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-portal-assign-upgrade-role-to-managed-identity-1.png" alt="Azure add custom role">
    </a>
    <figcaption>Assign upgrade role to your user assigned managed identity</figcaption>
  </figure>
</p>


!!! warning
    [When you assign roles or remove role assignments, it can take up to 30 minutes for changes to take effect.](https://docs.microsoft.com/en-us/azure/role-based-access-control/troubleshooting#role-assignment-changes-are-not-being-detected)

## Step 3: Add disk read permissions to your role connected to Hopsworks.ai

We require extra permission ("Microsoft.Compute/disks/read") to be added to the role you used to connect to Hopsworks.ai, the one that you have created in [Getting started Step 1.2](../getting_started/#step-12-creating-a-custom-role-for-hopsworksai). 
If you don't remember the name of the role that you have created in [Getting started Step 1.2](../getting_started/#step-12-creating-a-custom-role-for-hopsworksai), you can navigate to your Resource group, (1) click on *Access Control*, (2) navigate to the *Check Access* tab, (3) search for *hopsworks.ai*, (4) click on it, (5) now you have the name of your custom role used to connect to hopsworks.ai. 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-get-connected-hopswork.ai-role.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-get-connected-hopswork.ai-role.png" alt="Get your connected role to hopswork.ai">
    </a>
    <figcaption>Get your role connected to hopswork.ai</figcaption>
  </figure>
</p>

To edit the permissions associated with your role, stay on the same *Access Control* page, (1) click on the *Roles* tab, (2) search for your role name (the one you obtained above), (3) click on **...**, (4) click on *Edit*.


<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role.png" alt="Edit your connected role to hopswork.ai">
    </a>
    <figcaption>Edit your role connected to hopswork.ai</figcaption>
  </figure>
</p>

You will arrive at the *Update a custom role* page as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-1.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-1.png" alt="Edit your connected role to hopswork.ai 1">
    </a>
    <figcaption>Edit your role connected to hopswork.ai</figcaption>
  </figure>
</p>

Navigate to the *JSON* tab, then click on *Edit*, as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-2.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-2.png" alt="Edit your connected role to hopswork.ai 2">
    </a>
    <figcaption>Edit your role connected to hopswork.ai</figcaption>
  </figure>
</p>

Now, add the missing permission *"Microsoft.Compute/disks/read"* to the list of actions, then click on *Save*, click on *Review + update*, and finally click on *Update*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-3.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-3.png" alt="Edit your connected role to hopswork.ai 3">
    </a>
    <figcaption>Add missing permissions to your role connected to hopswork.ai</figcaption>
  </figure>
</p>

## Step 4: Run the upgrade process

You need to click on *Upgrade* to start the upgrade process. You will be prompted with the screen shown below to confirm your intention to upgrade: 

!!! note
    No need to worry about the following message since this is done already in [Step 2](#step-2-add-upgrade-permissions-to-your-user-assigned-managed-identity)

    **Make sure that your user assigned managed identity (hopsworks-doc-identity) includes the following permissions:
    [ "Microsoft.Compute/virtualMachines/read", "Microsoft.Compute/virtualMachines/write", "Microsoft.Compute/disks/read", "Microsoft.Compute/disks/write" ]**

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-upgrade-prompt.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-upgrade-prompt.png" alt="Azure Upgrade Prompt">
    </a>
    <figcaption>Upgrade confirmation</figcaption>
  </figure>
</p>

Check the *Yes, upgrade cluster* checkbox to proceed, then the *Upgrade* button will be activated as shown below:

!!! warning
    Currently, we only support upgrade for the head node and you will need to recreate your workers once the upgrade is successfully completed. 


<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-upgrade-prompt-1.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-upgrade-prompt-1.png" alt="Azure Upgrade Prompt">
    </a>
    <figcaption>Upgrade confirmation</figcaption>
  </figure>
</p>


Depending on how big your current cluster is, the upgrade process may take from 1 hour to a few hours until completion.

!!! note
    We don't delete your old cluster until the upgrade process is successfully completed. 


<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-upgrade-start.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-upgrade-start.png" alt="Azure Upgrade starting">
    </a>
    <figcaption>Upgrade is running</figcaption>
  </figure>
</p>

Once the upgrade is completed, you can confirm that you have the new Hopsworks version by checking the *Details* tab of your cluster as below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-upgrade-complete.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-upgrade-complete.png" alt="Azure Upgrade complete">
    </a>
    <figcaption>Upgrade is complete</figcaption>
  </figure>
</p>

## Error handling
There are two categories of errors that you may encounter during an upgrade. First, a permission error due to a missing permission in your role connected to Hopsworks.ai, see [Error 1](#error-1-missing-permissions-error). Second, an error during the upgrade process running on your cluster, see [Error 2](#error-2-upgrade-process-error).

### Error 1: Missing permissions error
If you encounter the following permission error right after starting an upgrade, then you need to make sure that the role you used to connect to Hopsworks.ai, the one that you have created in [Getting started Step 1.2](../getting_started/#step-12-creating-a-custom-role-for-hopsworksai), have permissions to read and write disks
*("Microsoft.Compute/disks/read", "Microsoft.Compute/disks/write")*. 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-upgrade-permission-error.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-upgrade-permission-error.png" alt="Azure upgrade permission error">
    </a>
    <figcaption>Missing permission error</figcaption>
  </figure>
</p>


If you don't remember the name of the role that you have created in [Getting started Step 1.2](../getting_started/#step-12-creating-a-custom-role-for-hopsworksai), you can navigate to your Resource group, (1) click on *Access Control*, (2) navigate to the *Check Access* tab, (3) search for *hopsworks.ai*, (4) click on it, (5) now you have the name of your custom role used to connect to hopsworks.ai. 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-get-connected-hopswork.ai-role.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-get-connected-hopswork.ai-role.png" alt="Get your connected role to hopswork.ai">
    </a>
    <figcaption>Get your role connected to hopswork.ai</figcaption>
  </figure>
</p>

To edit the permissions associated with your role, stay on the same *Access Control* page, (1) click on the *Roles* tab, (2) search for your role name (the one you obtained above), (3) click on **...**, (4) click on *Edit*.


<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role.png" alt="Edit your connected role to hopswork.ai">
    </a>
    <figcaption>Edit your role connected to hopswork.ai</figcaption>
  </figure>
</p>

You will arrive at the *Update a custom role* page as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-1.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-1.png" alt="Edit your connected role to hopswork.ai 1">
    </a>
    <figcaption>Edit your role connected to hopswork.ai</figcaption>
  </figure>
</p>

Navigate to the *JSON* tab, then click on *Edit*, as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-2.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-2.png" alt="Edit your connected role to hopswork.ai 2">
    </a>
    <figcaption>Edit your role connected to hopswork.ai</figcaption>
  </figure>
</p>

In our example, we were missing only the read permission ("Microsoft.Compute/disks/read"). First, add the missing permission, then click on *Save*, click on *Review + update*, and finally click on *Update*.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-3.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-edit-connected-hopsworks.ai-role-3.png" alt="Edit your connected role to hopswork.ai 3">
    </a>
    <figcaption>Add missing permissions to your role connected to hopswork.ai</figcaption>
  </figure>
</p>

Once you have updated your role, click on *Retry* to retry the upgrade process.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-upgrade-permission-error.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-upgrade-permission-error.png" alt="Azure upgrade permission error">
    </a>
    <figcaption>Retry the upgrade process</figcaption>
  </figure>
</p>

### Error 2: Upgrade process error

If an error occurs during the upgrade process, you will have the option to rollback to your old cluster as shown below: 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-upgrade-error.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-upgrade-error.png" alt="Error during upgrade">
    </a>
    <figcaption>Error occurred during upgrade</figcaption>
  </figure>
</p>

Click on *Rollback* to recover your old cluster before upgrade.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-rollback-prompt-1.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-rollback-prompt-1.png" alt="Rollback prompt">
    </a>
    <figcaption>Upgrade rollback confirmation</figcaption>
  </figure>
</p>

Check the *Yes, rollback cluster* checkbox to proceed, then the *Rollback* button will be activated as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-rollback-prompt-2.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-rollback-prompt-2.png" alt="Rollback prompt">
    </a>
    <figcaption>Upgrade rollback confirmation</figcaption>
  </figure>
</p>

Once the rollback is completed, you will be able to continue working as normal with your old cluster.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/azure/azure-notification-stopped.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/azure/azure-notification-stopped.png" alt="New version notification">
    </a>
    <figcaption>Rollback succeed</figcaption>
  </figure>
</p>
