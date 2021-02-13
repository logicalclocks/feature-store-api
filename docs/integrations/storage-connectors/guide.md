# Storage Connectors

You can define a storage connector in Hopsworks for batch and streaming data sources. External (on-demand) Feature Groups can also be defined with storage connectors, where only the metadata is stored in Hopsworks. You will need to authenticate against your external data source to be able to read data from it. Our storage connectors provide two main mechanisms for authentication: using credentials or an authentication role (IAM Role on AWS or Managed Identity on Azure). Hopsworks supports both a single IAM role or Managed Identity for the whole Hopsworks cluster or more advanced multiple IAM roles or Managed Identities that can only be assumed by users with a specific role in a specific project.

!!! info "Multiple IAM Roles/Managed Identities"
    In the Admin Panel for Hopsworks, you can specify a Cloud Role (IAM role or managed identity) and (1) in which Project and (2) what role within that Project can assume that Cloud Role. For example, you could limit access to an IAM Role with Redshift access to users who have the 'Data Owner' Role in a Project called 'RawFeatures'. 

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
