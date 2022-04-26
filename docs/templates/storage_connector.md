# Storage Connector

Storage connectors encapsulate all information needed for the execution engine
to read and write to specific storage. This storage can be S3, a JDBC compliant
database or the distributed filesystem HOPSFS.

## Retrieval

{{sc_get}}

## HopsFS

### Properties

{{hopsfs_properties}}

### Methods

{{hopsfs_methods}}

## JDBC

### Properties

{{jdbc_properties}}

### Methods

{{jdbc_methods}}

## S3

### Properties

{{s3_properties}}

### Methods

{{s3_methods}}

## Redshift

### Properties

{{redshift_properties}}

### Methods

{{redshift_methods}}

## Azure Data Lake Storage

### Properties

{{adls_properties}}

### Methods

{{adls_methods}}

## Snowflake

### Properties

{{snowflake_properties}}

### Methods

{{snowflake_methods}}

## Google Cloud Storage

### Properties

{{gcs_properties}}

### Methods

{{gcs_methods}}

## BigQuery
The BigQuery storage connector provides integration to GCP BigQuery.
You can use it to run bigquery on your GCP and load results into spark dataframe.
The storage connector uses the Google `spark-bigquery-connector` behind the scenes.
To read more information by Google on the connector, like the spark options or usage, check [GCP documentation](https://github.com/GoogleCloudDataproc/spark-bigquery-connector#usage
'github.com/GoogleCloudDataproc/spark-bigquery-connector')

### Properties

{{bigquery_properties}}

### Methods

{{bigquery_methods}}
