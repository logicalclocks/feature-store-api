# Limiting AWS permissions

Hopsworks.ai requires a set of permissions to be able to manage resources in the user’s AWS account.
By default, these permissions are set to easily allow a wide range of different configurations and allow
us to automate as many steps as possible. While we ensure to never access resources we shouldn’t,
we do understand that this might not be enough for your organization or security policy.
This guide explains how to lock down AWS permissions following the IT security policy principle of least privilege allowing
Hopsworks.ai to only access resources in a specific VPC.

## Limiting the cross-account role permissions

### Step 1: Create a VPC

To restrict Hopsworks.ai from accessing resources outside of a specific VPC, you need to create a new VPC
connected to an Internet Gateway. This can be achieved in the AWS Management Console following this guide:
[Create the VPC](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-getting-started.html#getting-started-create-vpc).
The option VPC with a `Single Public Subnet` from the Launch VPC Wizard should work out of the box.
Alternatively, an existing VPC such as the default VPC can be used and Hopsworks.ai will be restricted to this VPC.
Note the VPC ID of the VPC you want to use for the following steps.

!!! note
    Make sure you enable `DNS hostnames` for your VPC

After you have created the VPC either [Create a Security Group](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html#CreatingSecurityGroups) or use VPC's default.

!!! note
    The [Security Group](https://docs.aws.amazon.com/vpc/latest/userguide/VPC_SecurityGroups.html#AddRemoveRules) and/or [Network ACLs](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-network-acls.html#Rules)
    need to be configured so that at least port `80` is reachable from the internet otherwise you will have to use self signed certificate in your Hopsworks cluster.

!!! note
    If you intend to use the managed users option on your Hopsworks cluster you should also add a rule to open port 443.

### Step 2: Create an instance profile

You need to create an instance profile that will identify all instances started by Hopsworks.ai.
Follow this guide to create a role to be used by EC2 with no permissions attached:
[Creating a Role for an AWS Service (Console)](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-service.html).
Take note of the ARN of the role you just created.

You will need to add permissions to the instance profile to give access to the S3 bucket where Hopsworks will store its data. For more details about these permissions check [our guide here](../getting_started/#step-2-creating-instance-profile).
Check [bellow](#limiting-the-instance-profile-permissions) for more information on restricting the permissions given the instance profile.

### Step 3: Set permissions of the cross-account role

During the account setup for Hopsworks.ai, you were asked to create and provide a cross-account role.
If you don’t remember which role you used then you can find it in Settings/Account Settings in Hopsworks.ai.
Edit this role in the AWS Management Console and overwrite the existing inline policy with the following policy.

Note that you have to replace `[INSTANCE_PROFILE_NAME]` and `[VPC_ID]` for multiple occurrences in the given policy.

If you want to learn more about how this policy works check out:
[How to Help Lock Down a User’s Amazon EC2 Capabilities to a Single VPC](https://aws.amazon.com/blogs/security/how-to-help-lock-down-a-users-amazon-ec2-capabilities-to-a-single-vpc/).

```json
{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "NonResourceBasedPermissions",
        "Effect": "Allow",
        "Action": [
          "ec2:DescribeInstances",
          "ec2:DescribeVpcs",
          "ec2:DescribeVolumes",
          "ec2:DescribeSubnets",
          "ec2:DescribeKeyPairs",
          "ec2:DescribeInstanceStatus",
          "iam:ListInstanceProfiles",
          "ec2:DescribeSecurityGroups",
          "ec2:DescribeVpcAttribute",
          "ec2:DescribeRouteTables"
        ],
        "Resource": "*"
      },
      {
        "Sid": "IAMPassRoleToInstance",
        "Effect": "Allow",
        "Action": "iam:PassRole",
        "Resource": "arn:aws:iam::*:role/[INSTANCE_PROFILE_NAME]"
      },
      {
        "Sid": "EC2RunInstancesOnlyWithGivenRole",
        "Effect": "Allow",
        "Action": "ec2:RunInstances",
        "Resource": "arn:aws:ec2:*:*:instance/*",
        "Condition": {
          "ArnLike": {
            "ec2:InstanceProfile": "arn:aws:iam::*:instance-profile/[INSTANCE_PROFILE_NAME]"
          }
        }
      },
      {
        "Sid": "EC2RunInstancesOnlyInGivenVpc",
        "Effect": "Allow",
        "Action": "ec2:RunInstances",
        "Resource": "arn:aws:ec2:*:*:subnet/*",
        "Condition": {
          "ArnLike": {
            "ec2:vpc": "arn:aws:ec2:*:*:vpc/[VPC_ID]"
          }
        }
      },
      {
        "Sid": "AllowInstanceActions",
        "Effect": "Allow",
        "Action": [
          "ec2:StopInstances",
          "ec2:TerminateInstances",
          "ec2:StartInstances",
          "ec2:CreateTags",
          "ec2:AssociateIamInstanceProfile"
        ],
        "Resource": "arn:aws:ec2:*:*:instance/*",
        "Condition": {
          "ArnLike": {
            "ec2:InstanceProfile": "arn:aws:iam::*:instance-profile/[INSTANCE_PROFILE_NAME]"
          }
        }
      },
      {
        "Sid": "RemainingRunInstancePermissions",
        "Effect": "Allow",
        "Action": "ec2:RunInstances",
        "Resource": [
          "arn:aws:ec2:*:*:volume/*",
          "arn:aws:ec2:*::image/*",
          "arn:aws:ec2:*::snapshot/*",
          "arn:aws:ec2:*:*:network-interface/*",
          "arn:aws:ec2:*:*:key-pair/*",
          "arn:aws:ec2:*:*:security-group/*"
        ]
      },
      {
        "Sid": "EC2VpcNonResourceSpecificActions",
        "Effect": "Allow",
        "Action": [
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:RevokeSecurityGroupIngress",
          "ec2:DeleteSecurityGroup"
        ],
        "Resource": "*",
        "Condition": {
          "ArnLike": {
            "ec2:vpc": "arn:aws:ec2:*:*:vpc/[VPC_ID]"
          }
        }
      },
      {
        "Sid": "EC2BackupCreation",
        "Effect": "Allow",
        "Action": [
          "ec2:CreateImage",
	        "ec2:DescribeImages",
	        "ec2:DeregisterImage",
	        "ec2:DeleteSnapshot",
	        "ec2:DescribeSnapshots"
        ],
        "Resource": "*"
      }
    ]
  }
```

### Step 4: Create your Hopsworks instance

You can now create a new Hopsworks instance in Hopsworks.ai by selecting the configured instance profile,
VPC and security group during instance configuration. Selecting any other VPCs or instance profiles will result in permissions errors.

### Step 5: Supporting multiple VPCs

The policy can be extended to give Hopsworks.ai access to multiple VPCs.
See: [Creating a Condition with Multiple Keys or Values](https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_multi-value-conditions.html).

### Backup permissions

The following permissions are only needed for the backup feature. You can remove them if you are not going to create backups or if you do not have access to this Enterprise feature.

```json
      {
        "Sid": "EC2BackupCreation",
        "Effect": "Allow",
        "Action": [
          "ec2:CreateImage",
	        "ec2:DescribeImages",
	        "ec2:DeregisterImage",
	        "ec2:DeleteSnapshot",
	        "ec2:DescribeSnapshots"
        ],
        "Resource": "*",
      }
```

### Other removable permissions

The following permissions are needed to give an early warning if your VPC and security groups are badly configured. You can remove them if you don't need it.
```json
          "ec2:DescribeVpcAttribute",
          "ec2:DescribeRouteTables"
```
The following permissions are used to let you close and open ports on your cluster from hopswork.ai, you can remove them if you do not want to open ports on your cluster or if you want to manually open ports in EC2.

```json
      {
        "Sid": "EC2VpcNonResourceSpecificActions",
        "Effect": "Allow",
        "Action": [
          "ec2:AuthorizeSecurityGroupIngress",
          "ec2:RevokeSecurityGroupIngress",
          "ec2:DeleteSecurityGroup"
        ],
        "Resource": "*",
        "Condition": {
          "ArnLike": {
            "ec2:vpc": "arn:aws:ec2:*:*:vpc/[VPC_ID]"
          }
        }
      }
```

## Limiting the instance profile permissions

If you do not intend to take backups or if you do not have access to this Enterprise feature you can remove the permissions that are only used by the backup feature when [configuring instance profile permissions](../getting_started/#step-2-creating-instance-profile) .
For this remove the following permissions from the instance profile:

```json
      "S3:PutLifecycleConfiguration", 
      "S3:GetLifecycleConfiguration", 
      "S3:PutBucketVersioning", 
```

Hopsworks put its logs in Amazon CloudWatch so that you can access them without having to ssh into the machine. If you are not interested in this feature you can remove the following from your instance profile policy:

```json
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData",
        "ec2:DescribeVolumes",
        "ec2:DescribeTags",
        "logs:PutLogEvents",
        "logs:DescribeLogStreams",
        "logs:DescribeLogGroups",
        "logs:CreateLogStream",
        "logs:CreateLogGroup"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "ssm:GetParameter"
      ],
      "Resource": "arn:aws:ssm:*:*:parameter/AmazonCloudWatch-*"
    }
```
