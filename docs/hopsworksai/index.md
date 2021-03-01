# Hopsworks.ai

Hopsworks.ai is our managed platform for running Hopsworks and the Feature Store
in the cloud. It integrates seamlessly with third-party platforms such as Databricks,
SageMaker, and KubeFlow.

## Getting started

To get started with deploying a cluster in you Azure or AWS environment, follow these guides: [Azure](azure/getting_started.md), [AWS](aws/getting_started.md).

If you need more details about hopsworks.ai cluster creation steps you can check the following guides: [Azure](azure/cluster_creation.md), [AWS](aws/cluster_creation.md).

## Limiting permissions

Hopsworks.ai requires a set of permissions to be able to manage resources in the user’s cloud account. By default, these permissions are set to easily allow a wide range of different configurations and allow us to automate as many steps as possible. While we ensure to never access resources we shouldn’t, we do understand that this might not be enough for your organization or security policy. The following guides explain how you can reduce the permissions you give to hopsworks.ai by doing some of the cluster creation steps manually: [Azure](azure/restrictive_permissions.md), [AWS](aws/restrictive_permissions.md).

## Integration with managed Kubernetes

When deploying a cluster you can set it up to use managed Kubernetes to run python jobs, Jupyter servers, and ML model serving in a scalable way. This guide provides step-by-step instructions on how to set up Kubernetes and start a cluster using it: [AWS](aws/eks_ecr_integration.md).

## Integration with third-party platforms

Once you have deployed a cluster with hopsworks.ai you can connect to it from third-party platforms such as [Databricks](../integrations/databricks/configuration.md), [AWS Sagemaker](../integrations/sagemaker.md), [Azure HDInsight](../integrations/hdinsight.md), etc. Go to [integrations](../setup.md) for more third-party platforms and details.


## Other

You can find more information on the following topics by clicking on the links:

* [GPU support](gpu_support.md)
* [Ading and removing workers](adding_removing_workers.md)
* [User management](user_management.md)
