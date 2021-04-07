# Upgrade existing clusters on Hopsworks.ai (AWS)
This guide shows you how to upgrade your existing Hopsworks cluster to a newer version of Hopsworks. First, a notification will appear on the top of your cluster when a new version is available as shown in the figure below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-notification-running.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-notification-running.png" alt="New version notification">
    </a>
    <figcaption>A new Hopsworks version is available</figcaption>
  </figure>
</p>

## Step 1: Stop your cluster 

You need to **Stop** your cluster to start the upgrade process. Once your cluster is stopped, the *Upgrade* button will appear as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-notification-stopped.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-notification-stopped.png" alt="New version notification">
    </a>
    <figcaption>A new Hopsworks version is available</figcaption>
  </figure>
</p>


## Step 2: Add upgrade permissions to your instance profile

We require extra permissions to be added to the instance profile attached to your cluster to proceed with the upgrade. First to get the name of your instance profile, click on the *Details* tab as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-instance-profile.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-instance-profile.png" alt="Getting the name of your instance profile">
    </a>
    <figcaption>Getting the name of your instance profile</figcaption>
  </figure>
</p>


Once you get your instance profile name, navigate to [AWS management console](https://console.aws.amazon.com/iam/home#), then click on *Roles* and then search for your role name and click on it.  Go to the *Permissions* tab, click on *Add inline policy*, and then go to the *JSON* tab. Paste the following snippet, click on *Review policy*, name it, and click *Create policy*.

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "UpgradePermissions",
            "Effect": "Allow",
            "Action": [
                "ec2:DescribeVolumes",
                "ec2:DetachVolume",
                "ec2:AttachVolume",
                "ec2:ModifyInstanceAttribute"
            ],
            "Resource": "*"
        }
    ]
}
```

## Step 3: Run the upgrade process

You need to click on *Upgrade* to start the upgrade process. You will be prompted with the screen shown below to confirm your intention to upgrade: 

!!! note
    No need to worry about the following message since this is done already in [Step 2](#step-2-add-upgrade-permissions-to-your-instance-profile)

    **Make sure that your instance profile (hopsworks-doc) includes the following permissions:
    [ "ec2:DetachVolume", "ec2:AttachVolume", "ec2:ModifyInstanceAttribute" ]**

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-upgrade-prompt-1.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-upgrade-prompt-1.png" alt="AWS Upgrade Prompt">
    </a>
    <figcaption>Upgrade confirmation</figcaption>
  </figure>
</p>

Check the *Yes, upgrade cluster* checkbox to proceed, then the *Upgrade* button will be activated as shown below:

!!! warning
    Currently, we only support upgrade for the head node and you will need to recreate your workers once the upgrade is successfully completed. 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-upgrade-prompt-2.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-upgrade-prompt-2.png" alt="AWS Upgrade Prompt">
    </a>
    <figcaption>Upgrade confirmation</figcaption>
  </figure>
</p>

Depending on how big your current cluster is, the upgrade process may take from 1 hour to a few hours until completion.

!!! note
    We don't delete your old cluster until the upgrade process is successfully completed. 


<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-upgrade-start.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-upgrade-start.png" alt="AWS Upgrade starting">
    </a>
    <figcaption>Upgrade is running</figcaption>
  </figure>
</p>

Once the upgrade is completed, you can confirm that you have the new Hopsworks version by checking the *Details* tab of your cluster as below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-upgrade-complete.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-upgrade-complete.png" alt="AWS Upgrade complete">
    </a>
    <figcaption>Upgrade is complete</figcaption>
  </figure>
</p>

## Error handling 
There are two categories of errors that you may encounter during an upgrade. First, a permission error due to missing permission or a misconfigured policy in your instance profile, see [Error 1](#error-1-misconfigured-upgrade-permissions). Second, an error during the upgrade process running on your cluster, see [Error 2](#error-2-upgrade-process-error).

### Error 1: Misconfigured upgrade permissions

During the upgrade process, Hopsworks.ai starts by validating your instance profile permissions to ensure that it includes the required upgrade permissions. If one or more permissions are missing, or if the resource is not set correctly, you will be notified with an error message and a *Retry* button will appear as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-upgrade-retry.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-upgrade-retry.png" alt="AWS Upgrade Retry">
    </a>
    <figcaption>Upgrade permissions are missing</figcaption>
  </figure>
</p>

Update you instance profile accordingly, then click *Retry*

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-upgrade-start.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-upgrade-start.png" alt="AWS Upgrade starting">
    </a>
    <figcaption>Upgrade is running</figcaption>
  </figure>
</p>

### Error 2: Upgrade process error

If an error occurs during the upgrade process, you will have the option to rollback to your old cluster as shown below: 

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-upgrade-error.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-upgrade-error.png" alt="Error during upgrade">
    </a>
    <figcaption>Error occurred during upgrade</figcaption>
  </figure>
</p>

Click on *Rollback* to recover your old cluster before upgrade.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-rollback-prompt-1.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-rollback-prompt-1.png" alt="Rollback prompt">
    </a>
    <figcaption>Upgrade rollback confirmation</figcaption>
  </figure>
</p>

Check the *Yes, rollback cluster* checkbox to proceed, then the *Rollback* button will be activated as shown below:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-rollback-prompt-2.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-rollback-prompt-2.png" alt="Rollback prompt">
    </a>
    <figcaption>Upgrade rollback confirmation</figcaption>
  </figure>
</p>

Once the rollback is completed, you will be able to continue working as normal with your old cluster.

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/aws/aws-notification-stopped.png">
      <img style="border: 1px solid #000" src="../../../assets/images/hopsworksai/aws/aws-notification-stopped.png" alt="New version notification">
    </a>
    <figcaption>Rollback succeed</figcaption>
  </figure>
</p>

