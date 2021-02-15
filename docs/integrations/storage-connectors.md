# Storage Connectors

You can define storage connectors in Hopsworks for batch and streaming data sources. Storage connectors securely store information in Hopsworks about how to securely connect to external data stores. They can be used in both programs and in Hopsworks to easily and securely connect and ingest data to the Feature Store. External (on-demand) Feature Groups can also be defined with storage connectors, where only the metadata is stored in Hopsworks. 

Storage connectors provide two main mechanisms for authentication: using credentials or an authentication role (IAM Role on AWS or Managed Identity on Azure). Hopsworks supports both a single IAM role (AWS) or Managed Identity (Azure) for the whole Hopsworks cluster or more advanced multiple IAM roles (AWS) or Managed Identities (Azure) that can only be assumed by users with a specific role in a specific project.

!!! info "Mapping IAM Roles to Projects/Roles in Hopsworks"
    In Hopsworks, [you can specify a Cloud Role (IAM role or managed identity)](https://hopsworks.readthedocs.io/en/latest/admin_guide/cloud_role_mapping.html) and (1) in which Project and (2) what role within that Project can assume that Cloud Role. For example, you could limit access to a given IAM Role to users who have the 'Data Owner' role in a Project called 'RawFeatures'. That IAM Role could provide read access to a Redshift database/table, providing fine-grained access to Redshift from selected users in selected projects in Hopsworks.

* [ADLS](./storage-connectors/adls.md)

* [HopsFS](./storage-connectors/hopsfs.md)

* [JDBC](./storage-connectors/jdbc.md)

* [Redshift](./storage-connectors/redshift.md)

* [S3](./storage-connectors/s3.md)

* [Snowflake](./storage-connectors/snowflake.md)

## Programmatic Connectors (Spark, Python, Java/Scala, Flink)

It is also possible to use the rich ecosystem of connectors available in programs run on Hopsworks. Just Spark has tens of open-source libraries for connecting to relational databases, key-value stores, file systems, object stores, search databases, and graph databases. In Hopsworks, you can securely save your credentials as secrets, and securely access them with API calls when you need to connect to your external store. 

## Next Steps

For more information about how to use the Feature Store, see the [Quickstart Guide](../quickstart.md).

