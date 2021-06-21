# Hopsworks.ai Terraform Provider

Hopsworks.ai allows users to create and manage their clusters using the [Hopsworks.ai terraform provider](https://registry.terraform.io/providers/logicalclocks/hopsworksai/latest). In this guide, we first provide brief description on how to get started on AWS and AZURE, then we show how to import an existing cluster to be managed by terraform.

## Getting Started with AWS

Complete the following steps to start using Hopsworks.ai Terraform Provider on AWS.

1. Create a Hopsworks.ai API KEY as described in details [here](./api_key.md), and export the API KEY as follows 
```bash
export HOPSWORKSAI_API_KEY=<YOUR_API_KEY>
```
2. Download the proper Terraform CLI for your os from [here](https://www.terraform.io/downloads.html).
3. Install the [AWS CLI](https://aws.amazon.com/cli/) and run `aws configurre` to configure your AWS credentials.

### Example 
In this section, we provide a simple example to create a Hopsworks cluster on AWS along with all its required resources (ssh key, S3 bucket, and instance profile with the required permissions). 

1. In your terminal, run the following to create a demo directory and cd to it 
```bash
mkdir demo
cd demo
```
2. In this empty directory, create an empty file `main.tf`. Open the file and paste the following configurations to it then save it.
```hcl
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">=3.42.0"
    }
    hopsworksai = {
      source = "logicalclocks/hopsworksai"
    }
  }
}

variable "region" {
  type    = string
  default = "us-east-2"
}

provider "aws" {
  region = var.region
}

provider "hopsworksai" {
}

# Create the required aws resources, an ssh key, an s3 bucket, and an instance profile with the required hopsworks permissions
module "aws" {
  source = "logicalclocks/helpers/hopsworksai//modules/aws"
  region = var.region
}

# Create a cluster with no workers
resource "hopsworksai_cluster" "cluster" {
  name    = "tf-hopsworks-cluster"
  ssh_key = module.aws.ssh_key_pair_name

  head {
  }

  aws_attributes {
    region               = var.region
    bucket_name          = module.aws.bucket_name
    instance_profile_arn = module.aws.instance_profile_arn
  }

  open_ports {
    ssh = true
  }
}

output "hopsworks_cluster_url" {
  value = hopsworksai_cluster.cluster.url
}
```
3. Initialize the terraform directory by running the following command 
```bash
terraform init 
```
4. Now you can apply the changes to create all required resources
```bash
terraform apply 
```
5. Once terraform finishes creating the resources, it will output the url to the newly created cluster. Notice that for now, you have to navigate to your [Hopsworks.ai dashboard](https://managed.hopsworks.ai/dashboard) to get your login credentials.

6. After you finish working with the cluster, you can terminate it along with the other AWS resources using the following command
```bash
terraform destroy 
```


## Getting Started with AZURE

Complete the following steps to start using Hopsworks.ai Terraform Provider on AZURE.

1. Create a Hopsworks.ai API KEY as described in details [here](./api_key.md), and export the API KEY as follows 
```bash
export HOPSWORKSAI_API_KEY=<YOUR_API_KEY>
```
2. Download the proper Terraform CLI for your os from [here](https://www.terraform.io/downloads.html).
3. Install the [AZURE CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) and run `az login` to configure your AZURE credentials.

### Example 
In this section, we provide a simple example to create a Hopsworks cluster on AWS along with all its required resources (ssh key, S3 bucket, and instance profile with the required permissions). 

1. In your terminal, run the following to create a demo directory and cd to it 
```bash
mkdir demo
cd demo
```
2. In this empty directory, create an empty file `main.tf`. Open the file and paste the following configurations to it then save it.
```hcl
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = ">= 2.60.0"
    }
    hopsworksai = {
      source = "logicalclocks/hopsworksai"
    }
  }
}

variable "resource_group" {
  type    = string
}

provider "azurerm" {
  features {}
  skip_provider_registration = true
}

provider "hopsworksai" {
}

data "azurerm_resource_group" "rg" {
  name = var.resource_group
}

# Create the required azure resources, an ssh key, a storage account, and an user assigned managed identity with the required hopsworks permissions
module "azure" {
  source         = "logicalclocks/helpers/hopsworksai//modules/azure"
  resource_group = var.resource_group
}

# Create a cluster with no workers
resource "hopsworksai_cluster" "cluster" {
  name    = "tf-hopsworks-cluster"
  ssh_key = module.azure.ssh_key_pair_name

  head {
  }

 azure_attributes {
    location                       = module.azure.location
    resource_group                 = module.azure.resource_group
    storage_account                = module.azure.storage_account_name
    user_assigned_managed_identity = module.azure.user_assigned_identity_name
  }

  open_ports {
    ssh = true
  }
}

output "hopsworks_cluster_url" {
  value = hopsworksai_cluster.cluster.url
}
```
3. Initialize the terraform directory by running the following command 
```bash
terraform init 
```
4. Now you can apply the changes to create all required resources
```bash
terraform apply 
```
5. Once terraform finishes creating the resources, it will output the url to the newly created cluster. Notice that for now, you have to navigate to your [Hopsworks.ai dashboard](https://managed.hopsworks.ai/dashboard) to get your login credentials.

6. After you finish working with the cluster, you can terminate it along with the other AZURE resources using the following command
```bash
terraform destroy 
```

## Importing an existing cluster to terraform 

In this section, we show how to use `terraform import` to manage your existing Hopsworks cluster. 

* **Step 1**: In your [Hopsworks.ai dashboard](https://managed.hopsworks.ai/dashboard), choose the cluster you want to import to terraform, then go to the *Details* tab and copy the *Id* as shown in the figure below 

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/get-cluster-id.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/get-cluster-id.png" alt="Details tab">
    </a>
    <figcaption>Click on the Details tab and copy the Id</figcaption>
  </figure>
</p>

* **Step 2**: In your terminal, create an empty directory and cd to it.
```bash
mkdir import-demo
cd import-demo
```

* **Step 3**: In this empty directory, create an empty file `versions.tf`. Open the file and paste the following configurations. 

!!! note
    Notice that you need to change these configurations depending on your cluster, in this example, the Hopsworks cluster reside in region `us-east-2` on AWS.

```hcl
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">=3.42.0"
    }
    hopsworksai = {
      source = "logicalclocks/hopsworksai"
    }
  }
}

provider "aws" {
  region = us-east-2
}

provider "hopsworksai" {
}
```

* **Step 4**: Initialize the terraform directory by running the following command 
```bash
terraform init 
```

* **Step 5**: Create another file `main.tf`. Open the file and paste the following configuration.
```hcl
resource "hopsworksai_cluster" "cluster" {
}
```

* **Step 6**: Import the cluster state using `terraform import`, in this step you need the cluster id from Step 1 (33ae7ae0-d03c-11eb-84e2-af555fb63565).
```bash 
terraform import hopsworksai_cluster.cluster 33ae7ae0-d03c-11eb-84e2-af555fb63565
```

The output should be similar to the following snippet 
```bash
hopsworksai_cluster.cluster: Importing from ID "33ae7ae0-d03c-11eb-84e2-af555fb63565"...
hopsworksai_cluster.cluster: Import prepared!
  Prepared hopsworksai_cluster for import
hopsworksai_cluster.cluster: Refreshing state... [id=33ae7ae0-d03c-11eb-84e2-af555fb63565]

Import successful!

The resources that were imported are shown above. These resources are now in
your Terraform state and will henceforth be managed by Terraform.
```

* **Step 7**: At that moment the local terraform state is updated, however, if we try to run `terraform plan` or `terraform apply` it will complain about missing configurations. The reason is that our local resource configuration in `main.tf` is empty, we should populate it using the terraform state commands as shown below:
```bash
terraform show -no-color > main.tf
```

* **Step 8**: If you try to run `terraform plan` again, the command will complain that the read-only attributes are set (Computed attributes) as shown below. The solution is to remove these attributes from the `main.tf` and retry again until you have no errors.
```bash
Error: Computed attributes cannot be set

  on main.tf line 3, in resource "hopsworksai_cluster" "cluster":
   3:     activation_state               = "stoppable"

Computed attributes cannot be set, but a value was set for "activation_state".


Error: Computed attributes cannot be set

  on main.tf line 6, in resource "hopsworksai_cluster" "cluster":
   6:     cluster_id                     = "33ae7ae0-d03c-11eb-84e2-af555fb63565"

Computed attributes cannot be set, but a value was set for "cluster_id".


Error: Computed attributes cannot be set

  on main.tf line 7, in resource "hopsworksai_cluster" "cluster":
   7:     creation_date                  = "2021-06-18T15:51:07+02:00"

Computed attributes cannot be set, but a value was set for "creation_date".


Error: Invalid or unknown key

  on main.tf line 8, in resource "hopsworksai_cluster" "cluster":
   8:     id                             = "33ae7ae0-d03c-11eb-84e2-af555fb63565"



Error: Computed attributes cannot be set

  on main.tf line 13, in resource "hopsworksai_cluster" "cluster":
  13:     start_date                     = "2021-06-18T15:51:07+02:00"

Computed attributes cannot be set, but a value was set for "start_date".


Error: Computed attributes cannot be set

  on main.tf line 14, in resource "hopsworksai_cluster" "cluster":
  14:     state                          = "running"

Computed attributes cannot be set, but a value was set for "state".


Error: Computed attributes cannot be set

  on main.tf line 17, in resource "hopsworksai_cluster" "cluster":
  17:     url                            = "https://33ae7ae0-d03c-11eb-84e2-af555fb63565.dev-cloud.hopsworks.ai/hopsworks/#!/"

Computed attributes cannot be set, but a value was set for "url".
```

* **Step 9**: Once you have fixed all the errors, you should get the following output when running `terraform plan`. With that, you can proceed as normal to manage this cluster locally using terraform.
```bash
hopsworksai_cluster.cluster: Refreshing state... [id=33ae7ae0-d03c-11eb-84e2-af555fb63565]

No changes. Infrastructure is up-to-date.

This means that Terraform did not detect any differences between your
configuration and real physical resources that exist. As a result, no
actions need to be performed.
```

## Next Steps
* Check the [Hopsworks.ai terraform provider documentation](https://registry.terraform.io/providers/logicalclocks/hopsworksai/latest/docs) for more details about the different resources and data sources supported by the provider and a description of their attributes.
* Check the [Hopsworks.ai terraform AWS examples](https://github.com/logicalclocks/terraform-provider-hopsworksai/tree/main/examples/complete/aws), each example contains a README file describing how to run it and more details about configuring it.
* Check the [Hopsworks.ai terraform AZURE examples](https://github.com/logicalclocks/terraform-provider-hopsworksai/tree/main/examples/complete/azure), each example contains a README file describing how to run it and more details about configuring it.
