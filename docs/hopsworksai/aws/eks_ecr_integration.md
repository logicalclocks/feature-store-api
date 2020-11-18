# Integration with Amazon EKS and Amazon ECR

This guide shows how to create a cluster in Hopsworks.ai with integrated support for Amazon Elastic Kubernetes Service (EKS) and Amazon Elastic Container Registry (ECR). So that Hopsworks can launch Python jobs, Jupyter servers, and ML model servings on top of Amazon EKS.

!!! warning
    In the current version, we don't support sharing EKS clusters between Hopsworks clusters. That is, an EKS cluster can be only used by one Hopsworks cluster.

## Step 1: Create an EKS cluster on AWS

If you have an existing EKS cluster, skip this step and go directly to Step 2.

Amazon provides two getting started guides using [AWS management console](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-console.html) or [`eksctl`](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html) to help you create an EKS cluster.
The easiest way is to use the eksctl command.

### Step 1.1: Installing eksctl, aws, and kubectl

Follow the prerequisites section in [getting started with `eksctl`](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html) to install aws, eksctl, and kubectl.

### Step 1.2: Create an EKS cluster using eksctl

You can create a sample EKS cluster with the name *my-eks-cluster* using Kubernetes version *1.17* with *2* managed nodes in the *us-east-2* region by running the following command. For more details on the eksctl usage, check the [`eksctl` documentation](https://eksctl.io/usage/creating-and-managing-clusters/).

```bash
eksctl create cluster --name my-eks-cluster --version 1.17 --region us-east-2 --nodegroup-name my-nodes --nodes 2 --managed
```

Output:

```bash
[ℹ]  eksctl version 0.26.0
[ℹ]  using region us-east-2
[ℹ]  setting availability zones to [us-east-2b us-east-2a us-east-2c]
[ℹ]  subnets for us-east-2b - public:192.168.0.0/19 private:192.168.96.0/19
[ℹ]  subnets for us-east-2a - public:192.168.32.0/19 private:192.168.128.0/19
[ℹ]  subnets for us-east-2c - public:192.168.64.0/19 private:192.168.160.0/19
[ℹ]  using Kubernetes version 1.17
[ℹ]  creating EKS cluster "my-eks-cluster" in "us-east-2" region with managed nodes
[ℹ]  will create 2 separate CloudFormation stacks for cluster itself and the initial managed nodegroup
[ℹ]  if you encounter any issues, check CloudFormation console or try 'eksctl utils describe-stacks --region=us-east-2 --cluster=my-eks-cluster'
[ℹ]  CloudWatch logging will not be enabled for cluster "my-eks-cluster" in "us-east-2"
[ℹ]  you can enable it with 'eksctl utils update-cluster-logging --region=us-east-2 --cluster=my-eks-cluster'
[ℹ]  Kubernetes API endpoint access will use default of {publicAccess=true, privateAccess=false} for cluster "my-eks-cluster" in "us-east-2"
[ℹ]  2 sequential tasks: { create cluster control plane "my-eks-cluster", 2 sequential sub-tasks: { no tasks, create managed nodegroup "my-nodes" } }
[ℹ]  building cluster stack "eksctl-my-eks-cluster-cluster"
[ℹ]  deploying stack "eksctl-my-eks-cluster-cluster"
[ℹ]  building managed nodegroup stack "eksctl-my-eks-cluster-nodegroup-my-nodes"
[ℹ]  deploying stack "eksctl-my-eks-cluster-nodegroup-my-nodes"
[ℹ]  waiting for the control plane availability...
[✔]  saved kubeconfig as "/Users/maism/.kube/config"
[ℹ]  no tasks
[✔]  all EKS cluster resources for "my-eks-cluster" have been created
[ℹ]  nodegroup "my-nodes" has 2 node(s)
[ℹ]  node "ip-192-168-21-142.us-east-2.compute.internal" is ready
[ℹ]  node "ip-192-168-62-117.us-east-2.compute.internal" is ready
[ℹ]  waiting for at least 2 node(s) to become ready in "my-nodes"
[ℹ]  nodegroup "my-nodes" has 2 node(s)
[ℹ]  node "ip-192-168-21-142.us-east-2.compute.internal" is ready
[ℹ]  node "ip-192-168-62-117.us-east-2.compute.internal" is ready
[ℹ]  kubectl command should work with "/Users/maism/.kube/config", try 'kubectl get nodes'
[✔]  EKS cluster "my-eks-cluster" in "us-east-2" region is ready
```

Once the cluster is created, eksctl will write the cluster credentials for the newly created cluster to your local kubeconfig file (~/.kube/config).
To test the cluster credentials, you can run the following command to get list of nodes in the cluster.

```bash
kubectl get nodes
```

Output:

```bash
NAME                                           STATUS   ROLES    AGE     VERSION
ip-192-168-21-142.us-east-2.compute.internal   Ready    <none>   2m35s   v1.17.9-eks-4c6976
ip-192-168-62-117.us-east-2.compute.internal   Ready    <none>   2m34s   v1.17.9-eks-4c6976
```

## Step 2: Create an instance profile role on AWS

You need to create an instance profile role to allow instances created by Hopsworks.ai to access EKS and ECR.
To create a role, click on the following [link](https://console.aws.amazon.com/iam/home#/roles$new?step=type&roleType=aws&selectedService=EC2&selectedUseCase=EC2). Alternatively, you can go to the Roles section of the IAM service in AWS management console, click on *Create role*, choose *AWS Service* as the type of trusted entity, and then choose *EC2* from Common use cases. Then, click on *Next: Permissions*, *Next: Tags*, *Next: Review*, and then name your role and click *Create role*.
Navigate to your newly created role in [AWS management console](https://console.aws.amazon.com/iam/home#/roles) by searching for your role name and click on it. Go to the *Permissions* tab, click on *Add inline policy*, and then go to the *JSON* tab. Paste the following snippet, click on *Review policy*, name it, and click *Create policy*. Finally, copy your Role ARN (you will need it in the next steps).

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowPullMainImages",
            "Effect": "Allow",
            "Action": [
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchGetImage"
            ],
            "Resource": [
                "arn:aws:ecr:*:*:repository/filebeat",
                "arn:aws:ecr:*:*:repository/base"
            ]
        },
        {
            "Sid": "AllowPushandPullImages",
            "Effect": "Allow",
            "Action": [
                "ecr:CreateRepository",
                "ecr:GetDownloadUrlForLayer",
                "ecr:BatchGetImage",
                "ecr:CompleteLayerUpload",
                "ecr:UploadLayerPart",
                "ecr:InitiateLayerUpload",
                "ecr:DeleteRepository",
                "ecr:BatchCheckLayerAvailability",
                "ecr:PutImage",
                "ecr:ListImages",
                "ecr:BatchDeleteImage",
                "ecr:GetLifecyclePolicy",
                "ecr:PutLifecyclePolicy"
            ],
            "Resource": [
                "arn:aws:ecr:*:*:repository/*/filebeat",
                "arn:aws:ecr:*:*:repository/*/base"
            ]
        },
        {
            "Sid": "AllowGetAuthToken",
            "Effect": "Allow",
            "Action": "ecr:GetAuthorizationToken",
            "Resource": "*"
        },
        {
            "Sid": "AllowDescirbeEKS",
            "Effect": "Allow",
            "Action": "eks:DescribeCluster",
            "Resource": "arn:aws:eks:*:*:cluster/*"
        },
        {
            "Sid": "HopsFSS3Permissions",
            "Effect": "Allow",
            "Action": [
                "S3:PutObject",
                "S3:ListBucket",
                "S3:GetBucketLocation",
                "S3:GetObject",
                "S3:DeleteObject",
                "S3:AbortMultipartUpload",
                "S3:ListBucketMultipartUploads"
            ],
            "Resource": [
                "arn:aws:s3:::bucket.name/*",
                "arn:aws:s3:::bucket.name"
            ]
        }
    ]
}
```

Do not forget to replace *bucket.name* with appropriate S3 bucket name.

## Step 3: Allow your role to use your EKS cluster

You need to give your role permissions to access your EKS cluster using the following kubectl command. For more details, check [Managing users or IAM roles for your cluster](https://docs.aws.amazon.com/eks/latest/userguide/add-user-role.html). The kubectl edit command uses *vi* editor by default, however, you can override this behaviour by setting *KUBE_EDITOR* to your preferred editor, check [Kubernetes editing resources](https://kubernetes.io/docs/reference/kubectl/cheatsheet/#editing-resources).

```bash
KUBE_EDITOR="vi" kubectl edit configmap aws-auth -n kube-system
```

Output:

```bash
# Please edit the object below. Lines beginning with a '#' will be ignored,
# and an empty file will abort the edit. If an error occurs while saving this file will be
# reopened with the relevant failures.
#
apiVersion: v1
data:
mapRoles: |
    - groups:
        - system:bootstrappers
        - system:nodes
        rolearn: arn:aws:iam::xxxxxxxxxxxx:role/eksctl-my-eks-cluster-nodegroup-m-NodeInstanceRole-FQ7L0HQI4NCC
        username: system:node:{{EC2PrivateDNSName}}
kind: ConfigMap
metadata:
creationTimestamp: "2020-08-24T07:42:31Z"
name: aws-auth
namespace: kube-system
resourceVersion: "770"
selfLink: /api/v1/namespaces/kube-system/configmaps/aws-auth
uid: c794b2d8-9f10-443d-9072-c65d0f2eb552
```

Follow the example below (lines 13-16) to add your role to *mapRoles* and assign *system:masters* group to your role. Make sure to replace 'YOUR ROLE RoleARN' with your role RoleARN before saving.

!!! warning
    You need to use the RoleARN not the instance profile ARN, also make sure to keep the same formatting as in the example below.


```bash hl_lines="13 14 15 16"
# Please edit the object below. Lines beginning with a '#' will be ignored,
# and an empty file will abort the edit. If an error occurs while saving this file will be
# reopened with the relevant failures.
#
apiVersion: v1
data:
mapRoles: |
    - groups:
        - system:bootstrappers
        - system:nodes
        rolearn: arn:aws:iam::xxxxxxxxxxxx:role/eksctl-my-eks-cluster-nodegroup-m-NodeInstanceRole-FQ7L0HQI4NCC
        username: system:node:{{EC2PrivateDNSName}}
    - groups:
        - system:masters
        rolearn: <YOUR ROLE RoleARN>
        username: hopsworks
kind: ConfigMap
metadata:
creationTimestamp: "2020-08-24T07:42:31Z"
name: aws-auth
namespace: kube-system
resourceVersion: "770"
selfLink: /api/v1/namespaces/kube-system/configmaps/aws-auth
uid: c794b2d8-9f10-443d-9072-c65d0f2eb552
```

Once you are done with editing the configmap, save the updated config map.

```bash
configmap/aws-auth edited
```

##Step 4: Open Hopsworks required ports on your EKS cluster security group

You need to open the HTTP (80) and HTTPS (443) ports on the security group of your EKS cluster.
First, you need to get the name of the security group of your EKS cluster by using the following eksctl command. Notice that you need to change the cluster name according to your setup in Step 1 or if you have an existing cluster.

```bash
eksctl utils describe-stacks --region=us-east-2 --cluster=my-eks-cluster | grep 'OutputKey: "ClusterSecurityGroupId"' -a1
```

Check the output for *OutputValue*, that will be the id of your EKS security group.

```bash
ExportName: "eksctl-my-eks-cluster-cluster::ClusterSecurityGroupId",
OutputKey: "ClusterSecurityGroupId",
OutputValue: "YOUR_EKS_SECURITY_GROUP_ID"
```

Once you get the security group id (YOUR_EKS_SECURITY_GROUP_ID), you need to proceed to the AWS management console by clicking on *security groups*. Filter security groups using the *Security Group ID* and then paste your EKS security group id. Click on the *inbound rules* tab, then click on the *Edit inbound rules*, now you should arrive at the following screen.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-sg-edit-inbound-rules.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-sg-edit-inbound-rules.png" alt="Edit inbound rules">
    </a>
    <figcaption>Edit inbound rules</figcaption>
  </figure>
</p>

Add two rules for HTTP and HTTPS as follows:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-sg-hopsworks-inbound-rules.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-sg-hopsworks-inbound-rules.png" alt="Edit inbound rules">
    </a>
    <figcaption>Edit inbound rules</figcaption>
  </figure>
</p>

Click *Save rules* to save the updated rules to the security group.

## Step 5: Allow Hopsworks.ai to delete ECR repositories on your behalf

You need to add another inline policy to your role or user connected to Hopsworks.ai, see [../getting_started.md].
First, navigate to [AWS management console](https://console.aws.amazon.com/iam/home#), then click on *Roles* or *Users* depending on which connection method you have used in Hopsworks.ai, and then search for your role or user name and click on it.  Go to the *Permissions* tab, click on *Add inline policy*, and then go to the *JSON* tab. Paste the following snippet, click on *Review policy*, name it, and click *Create policy*.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowDeletingECRRepositories",
            "Effect": "Allow",
            "Action": [
                "ecr:DeleteRepository"
            ],
            "Resource": [
                "arn:aws:ecr:*:*:repository/*/filebeat",
                "arn:aws:ecr:*:*:repository/*/base"
            ]
        }
    ]
}
```

## Step 6: Create a Hopsworks cluster with EKS and ECR support

In Hopsworks.ai, select *Create cluster*. Choose the region of your EKS cluster and fill in the name of your S3 bucket, then click Next:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster.png" alt="Create Hopsworks cluster">
    </a>
    <figcaption>Create Hopsworks cluster</figcaption>
  </figure>
</p>

Choose your preferred SSH key to use with the cluster, then click Next:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-0.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-0.png" alt="Choose SSH key">
    </a>
    <figcaption>Choose SSH key</figcaption>
  </figure>
</p>

Choose the instance profile role that you have created in Step 2, then click Next:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-1.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-1.png" alt="Choose instance profile role">
    </a>
    <figcaption>Choose instance profile role</figcaption>
  </figure>
</p>

Choose **Enabled** to enable the use of Amazon EKS and ECR:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-2.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-2.png" alt="Choose Enabled">
    </a>
    <figcaption>Choose Enabled</figcaption>
  </figure>
</p>

Add your EKS cluster name and update your AWS account id if you want to use another account for ECR, then click Next:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-3.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-3.png" alt="Add EKS cluster name">
    </a>
    <figcaption>Add EKS cluster name</figcaption>
  </figure>
</p>

Choose the VPC of your EKS cluster, then click Next:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-4.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-4.png" alt="Choose VPC">
    </a>
    <figcaption>Choose VPC</figcaption>
  </figure>
</p>

Choose any of the subnets in the VPC, then click Next:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-5.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-5.png" alt="Choose Subnet">
    </a>
    <figcaption>Choose Subnet</figcaption>
  </figure>
</p>

Choose the security group that you have updated in Step 4, then click Next:

!!! note
    Select the Security Group in the form of eks-cluster-sg-YOUR-CLUSTER-NAME-* and NOT the ones for ControlPlaneSecurity or ClusterSharedNode.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-6.png">
      <img src="../../../assets/images/hopsworksai/aws/eks-hopsworks-create-cluster-6.png" alt="Choose Security Group">
    </a>
    <figcaption>Choose Security Group</figcaption>
  </figure>
</p>

Click *Review and submit*, then Create. Once the cluster is created, Hopsworks will use EKS to launch Python jobs, Jupyter servers, and ML model servings.
