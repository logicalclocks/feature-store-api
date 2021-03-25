# IAM role mapping

Using an EC2 instance profile enables your Hopsworks cluster to access AWS resources. This forces all Hopsworks users to
share the instance profile role and the resource access policies attached to that role. To allow for per project access policies
you could have your users use AWS credentials directly in their programs which is not recommended so you should instead use
[Role chaining](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_terms-and-concepts.html#iam-term-role-chaining).

To use Role chaining, you need to first setup IAM roles in AWS:

1. Create an instance profile role with policies that will allow it to assume all resource roles that we can assume from the Hopsworks
cluster.

```json

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AssumeDataRoles",
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Resource": [
                    "arn:aws:iam::123456789011:role/test-role",
                    "arn:aws:iam::xxxxxxxxxxxx:role/s3-role",
                    "arn:aws:iam::xxxxxxxxxxxx:role/dev-s3-role",
                    "arn:aws:iam::xxxxxxxxxxxx:role/redshift"
                ]
            }
        ]
    }
```
Example policy for assuming four roles.

2. Create the resource roles and edit trust relationship and add policy document that will allow the instance profile to assume this role.

```json

    {
        "Version": "2012-10-17",
        "Statement": [
            {
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::xxxxxxxxxxxx:role/instance-profile"
            },
            "Action": "sts:AssumeRole"
            }
        ]
    }
```
Example policy document.

3. Finally attach the instance profile to the master node of your Hopsworks AWS instance.


Role chaining allows the instance profile to assume any role in the policy attached in step 1. To limit access
to roles we can create a per project mapping from the admin page in hopsworks.

<p align="center">
  <figure>
    <img src="../../../assets/images/aws-role/goto-role-mapping.png" alt="Go to cloud role mapping.">
    <figcaption>Go to cloud role mapping.</figcaption>
  </figure>
</p>

By clicking the cloud role mapping icon in the admin page shown in the image above you can add mappings
by entering the project name, which roles in that project can access the cloud role and the role ARN.
Optionally you can set a role mapping as default by marking the default checkbox. The default roles can be changed from the project setting by a Data owner in that project.

<p align="center">
  <figure>
    <img src="../../../assets/images/aws-role/role-mappings.png" alt="Add cloud role to project mapping.">
    <figcaption>Add cloud role to project mapping.</figcaption>
  </figure>
</p>


Any member of a project can then go to the project settings page to see which roles they can assume.

<p align="center">
  <figure>
    <img src="../../../assets/images/aws-role/project-cloud-roles.png" alt="Cloud roles mapped to project.">
    <figcaption>Cloud roles mapped to project.</figcaption>
  </figure>
</p>

For instructions on how to use the assume role API see [assuming a role](../assume_role/#assuming-a-role)
