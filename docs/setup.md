# Integrations

![Feature Store Integrations](assets/images/integrations.png)

## Hopsworks

If you are using Spark or Python within Hopsworks, there is no further configuration required. Head over to the [Getting Started Guide](quickstart.md).

## Storage Connectors

Storage connectors encapsulate the configuration information needed for a Spark or Python execution engine to securely read and write to a specific storage. The [storage connector guide](integrations/storage-connectors.md) explains step by step how to configure different data sources (such as S3, Azure Data Lake, Redshift, Snowflake, any JDBC data source) and how they can be used to ingest data and define external (on-demand) Feature Groups.
 
## Databricks

Connecting to the Feature Store from Databricks requires setting up a Feature Store API Key for Databricks and installing one of the HSFS client libraries on your Databricks cluster. The [Databricks integration guide](integrations/databricks/configuration.md) explains step by step how to connect to the Feature Store from Databricks.

## AWS Sagemaker

Connecting to the Feature Store from SageMaker requires setting up a Feature Store API Key for SageMaker and installing the HSFS Python client library on SageMaker. The [AWS SageMaker integration guide](integrations/sagemaker.md) explains step by step how to connect to the Feature Store from SageMaker.

## Python (Local or KubeFlow)

Connecting to the Feature Store from any Python environment, such as your local environment or KubeFlow, requires setting up a Feature Store API Key and installing the HSFS Python client library. The [Python integration guide](integrations/python.md) explains step by step how to connect to the Feature Store from any Python environment.

## Spark Cluster

Connecting to the Feature Store from an external Spark cluster, such as Cloudera, requires configuring it with the Hopsworks client jars and configuration. The [Spark integration guide](integrations/spark.md) explains step by step how to connect to the Feature Store from an external Spark cluster.
