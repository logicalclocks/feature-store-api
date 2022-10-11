# Storage Connector

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
This storage connector provides integration to Google Cloud Storage (GCS).
Once you create a connector in FeatureStore, you can transact data from a GCS bucket into a spark dataframe
by calling the `read` API.

Authentication to GCP is handled by uploading the `JSON keyfile for service account` to the Hopsworks Project. For more information
on service accounts and creating keyfile in GCP, read [Google Cloud documentation.](https://cloud.google.com/docs/authentication/production#create_service_account
'creating service account keyfile')

The connector also supports the optional encryption method `Customer Supplied Encryption Key` by Google.
The encryption details are stored as `Secrets` in the FeatureStore for keeping it secure.
Read more about encryption on [Google Documentation.](https://cloud.google.com/storage/docs/encryption#customer-supplied_encryption_keys)

The storage connector uses the Google `gcs-connector-hadoop` behind the scenes. For more information, check out [Google Cloud Storage Connector for Spark and Hadoop](
https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/gcs#google-cloud-storage-connector-for-spark-and-hadoop 'google-cloud-storage-connector-for-spark-and-hadoop')

### Properties

{{gcs_properties}}

### Methods

{{gcs_methods}}

## BigQuery
The BigQuery storage connector provides integration to Google Cloud BigQuery.
You can use it to run bigquery on your GCP cluster and load results into spark dataframe by calling the `read` API.

Authentication to GCP is handled by uploading the `JSON keyfile for service account` to the Hopsworks Project. For more information
on service accounts and creating keyfile in GCP, read [Google Cloud documentation.](https://cloud.google.com/docs/authentication/production#create_service_account
'creating service account keyfile')

The storage connector uses the Google `spark-bigquery-connector` behind the scenes.
To read more about the spark connector, like the spark options or usage, check [Apache Spark SQL connector for Google BigQuery.](https://github.com/GoogleCloudDataproc/spark-bigquery-connector#usage
'github.com/GoogleCloudDataproc/spark-bigquery-connector')

### Properties

{{bigquery_properties}}

### Methods

{{bigquery_methods}}

## Kafka

### Properties

{{kafka_properties}}

### Methods

{{kafka_methods}}
