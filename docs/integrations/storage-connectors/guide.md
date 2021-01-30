# Storage Connectors

You can define a storage connector in Hopsworks for batch data sources . External (on-demand) Feature Groups can also be defined with storage connectors, where only the metadata is stored in Hopsworks.


## Credentials or IAM Roles?
You will need to authenticate against your external data source to be able to read data from it. Our storage connectors provide two main mechanisms for authentication: using credentials or an authentication role (IAM Role on AWS).
Hopsworks supports associating an IAM roles with either the whole cluster or in a more fine-grained manner with federated IAM Roles.

!!! info "Federated IAM Roles"
    In the Admin Panel for Hopsworks, you can link an existing IAM role (Cloud role) to a Project/Role in Hopsworks. In Hopsworks, only users who are a member of the linked project and have the specified role in that project will be allowed to assume the specified IAM role. For example, if i only wanted 'Data Owners' in the project 'supersecret' to be able to read from a specific table in Redshift, I would create an AWS IAM role with minimal privileges to read from the Redshift table and link it to the 'supersecret' project for users with the role 'Data Owner'. Now, Data Owners in supersecret can assume this IAM Role and read the data from Redshift, but no other users in the platform would have privileges to do so. The current supported project roles are 'Data Scientist' (read-only role) and 'Data Owner' (read/write/project-admin role).


[S3](./s3.md)
[Delta Lake](./s3.md)
[Redshift](./redshift.md)
[ADLS](./adls.md)
[Snowflake](./snowflake.md)
[JDBC](./jdbc.md)
[HopsFS](./hopsfs.md)


## Spark Connectors


## Next Steps

For more information about how to connect, see the [Connection](../generated/project.md) guide. Or continue with the Data Source guide to import your own data to the Feature Store.
