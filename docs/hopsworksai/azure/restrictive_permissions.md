# Limiting Azure permissions

Hopsworks.ai requires a set of permissions to be able to manage resources in the user’s Azure resource group.
By default, these permissions are set to easily allow a wide range of different configurations and allow
us to automate as many steps as possible. While we ensure to never access resources we shouldn’t,
we do understand that this might not be enough for your organization or security policy.
This guide explains how to lock down access permissions following the IT security policy principle of least privilege.

## Step 1: Create a virtual network and subnet

To restrict Hopsworks.ai from having write and delete access on virtual networks and subnet you need to create them manually.
This can be achieved in the Azure portal following this guide: [Create a virtual network](https://docs.microsoft.com/en-us/azure/virtual-network/quick-create-portal).
Make sure to use the resource group and location in which you intend to deploy your Hopsworks cluster. For the remaining of the configuration, the default options proposed by the portal should work out of the box.
Note the names of the virtual network and subnet you want to use for the following steps.

## Step 2: Create a network security group

To restrict Hopsworks.ai from having write and delete access on network security groups you need to create it manually.
This can be achieved in the Azure portal following this guide: [Create a network security group](https://docs.microsoft.com/en-us/azure/virtual-network/manage-network-security-group#create-a-network-security-group).
Make sure to use the resource group and location in which you intend to deploy your Hopsworks cluster.

For Hopsworks.ai to create the SSL certificates the network security group needs to allow inbound traffic on port 80.
For this, you need to add an inbound security rule to your network security group.
This can be achieved in the Azure portal following this guide: [Create a security rule](https://docs.microsoft.com/en-us/azure/virtual-network/manage-network-security-group#create-a-security-rule>).
Setting the destination port ranges to 80 and letting the default values for the other fields should work out of the box.

!!! note
    If you intend to use the managed users option on your Hopsworks cluster you should also add a rule to open port 443.

## Step 3: Set permissions of the cross-account role
During the account setup for Hopsworks.ai, you were asked to create create a custom role for your resource group.
Edit this role in the Azure portal by going to your resource group, clicking on *Access control (IAM)*, opening the tab *Roles*, searching for the role you created, clicking on the three dots at the end of the role line and clicking on edit.
You can then navigate to the JSON tab and overwrite the "action" field with the following:

```json
"actions": [
    "Microsoft.Compute/virtualMachines/*",
    "Microsoft.Compute/disks/write",
    "Microsoft.Compute/disks/delete",
    "Microsoft.Network/locations/*",
    "Microsoft.Network/networkInterfaces/*",
    "Microsoft.Network/networkSecurityGroups/read",
    "Microsoft.Network/networkSecurityGroups/join/action",
    "Microsoft.Network/networkSecurityGroups/defaultSecurityRules/read",
    "Microsoft.Network/networkSecurityGroups/securityRules/read",
    "Microsoft.Network/publicIPAddresses/join/action",
    "Microsoft.Network/publicIPAddresses/read",
    "Microsoft.Network/publicIPAddresses/write",
    "Microsoft.Network/publicIPAddresses/delete",
    "Microsoft.Network/virtualNetworks/read",
    "Microsoft.Network/virtualNetworks/subnets/read",
    "Microsoft.Network/virtualNetworks/subnets/join/action",
    "Microsoft.Resources/subscriptions/resourceGroups/read",
    "Microsoft.Compute/sshPublicKeys/read",
    "Microsoft.ManagedIdentity/userAssignedIdentities/assign/action",
    "Microsoft.ManagedIdentity/userAssignedIdentities/read"
  ]
```

## Step 4: Create your Hopsworks instance

You can now create a new Hopsworks instance in Hopsworks.ai by selecting the virtual network, subnet, and network security group during the instance configuration.
