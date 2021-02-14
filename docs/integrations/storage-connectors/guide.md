# Storage Connectors

You can define storage connectors in Hopsworks for batch and streaming data sources. Storage connectors securely store information in Hopsworks about how to securely connect to external data stores. They can be used in both programs and in Hopsworks to easily and securely connect and ingest data to the Feature Store. External (on-demand) Feature Groups can also be defined with storage connectors, where only the metadata is stored in Hopsworks. 

Storage connectors provide two main mechanisms for authentication: using credentials or an authentication role (IAM Role on AWS or Managed Identity on Azure). Hopsworks supports both a single IAM role (AWS) or Managed Identity (Azure) for the whole Hopsworks cluster or more advanced multiple IAM roles (AWS) or Managed Identities (Azure) that can only be assumed by users with a specific role in a specific project.

!!! info "Multiple IAM Roles/Managed Identities"
    In the Admin Panel for Hopsworks, you can specify a Cloud Role (IAM role or managed identity) and (1) in which Project and (2) what role within that Project can assume that Cloud Role. For example, you could limit access to an IAM Role with Redshift access to users who have the 'Data Owner' Role in a Project called 'RawFeatures'. 

[S3](./s3.md)
[Delta Lake](./s3.md)
[Redshift](./redshift.md)
[ADLS](./adls.md)
[Snowflake](./snowflake.md)
[JDBC](./jdbc.md)
[HopsFS](./hopsfs.md)


## Programmatic Connectors (Spark, Python, Java/Scala, Flink)

It is also possible to use the rich ecosystem of connectors available in programs run on Hopsworks. Just Spark has tens of open-source libraries for connecting to relational databases, key-value stores, file systems, object stores, search databases, and graph databases. In Hopsworks, you can securely save your credentials as secrets, and securely access them with API calls when you need to connect to your external store. 

## Next Steps

For more information about how to connect, see the [Connection](../generated/project.md) guide. Or continue with the Data Source guide to import your own data to the Feature Store.
