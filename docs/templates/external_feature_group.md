#External Feature Groups

External Feature Groups are Feature Groups for which the data is stored on an external storage system (e.g. Data Warehouse, S3, ADLS).
From an API perspective, external feature groups can be used in the same way as regular feature groups. Users can pick features from external feature groups to create training datasets. External feature groups can be also used as data source to create derived features, meaning features on which additional feature engineering is applied.

External feature groups rely on [Storage Connectors](../../integrations/storage-connectors/) to identify the location and to authenticate with the external storage.
When the external feature group is defined on top of an external database capabale of running SQL statements (i.e. when using the JDBC, Redshift or Snowflake connectors), the external feature group needs to be defined as a SQL statement. SQL statements can contain feature engineering transformations, when reading the external feature group, the SQL statement is pushed down to the storage for execution.

=== "Python"

    !!! example "Define a SQL based external feature group"
        ```python
        # Retrieve the storage connector defined before
        redshift_conn = fs.get_storage_connector("telco_redshift_cluster")
        telco_ext = fs.create_external_feature_group(name="telco_redshift",
                                                version=1,
                                                query="select * from telco",
                                                description="External feature group for telecom customer data",
                                                storage_connector=redshift_conn,
                                                statistics_config=True)
        telco_ext.save()
        ```

=== "Scala"

    !!! example "Connecting from Hopsworks"
        ```scala
        val redshiftConn = fs.getRedshiftConnector("telco_redshift_cluster")
        val telcoExt = (fs.createExternalFeatureGroup()
                    .name("telco_redshift_scala")
                    .version(2)
                    .query("select * from telco")
                    .description("External feature group for telecom customer data")
                    .storageConnector(redshiftConn)
                    .statisticsEnabled(true)
                    .build())
        telcoExt.save()
        ```


When defining an external feature group on top of a object store/external filesystem (i.e. when using the S3 or the ADLS connector) the underlying data is required to have a schema. The underlying data can be stored in ORC, Parquet, Delta, Hudi or Avro, and the schema for the feature group will be extracted by the files metadata.

=== "Python"

    !!! example "Define a SQL based external feature group"
        ```python
        # Retrieve the storage connector defined before
        s3_conn = fs.get_storage_connector("telco_s3_bucket")
        telco_ext = fs.create_external_feature_group(name="telco_s3",
                                                version=1,
                                                data_format="parquet",
                                                description="External feature group for telecom customer data",
                                                storage_connector=s3_conn,
                                                statistics_config=True)
        telco_ext.save()
        ```

=== "Scala"

    !!! example "Connecting from Hopsworks"
        ```scala
        val s3Conn = fs.getS3Connector("telco_s3_bucket")
        val telcoExt = (fs.createExtrenalFeatureGroup()
                    .name("telco_s3")
                    .version(1)
                    .dataFormat(ExternalDataFormat.PARQUET)
                    .description("External feature group for telecom customer data")
                    .storageConnector(s3Conn)
                    .statisticsEnabled(true)
                    .build())
        telcoExt.save()
        ```

## Use cases

There are two use cases in which a user can benefit from external feature groups:

- **Existing feature engineering pipelines**: in case users have recently migrated to Hopsworks Feature Store and they have existing feature engineering pipelines in production. Users can register the output of the existing pipelines as external feature groups in Hopsworks, and immediately use their features to build training datasets. With external feature groups, users do not have to modify the existing pipelines to write to the Hopsworks Feature Store.

- **Data Ingestion**: external feature groups can be used as a data source. The benefit of using external feature groups to ingest data from external sources is that the Hopsworks Feature Store keeps track of where the data is located and how to authenticate with the external storage system. In addition to that, the Hopsworks Feature Store tracks also the schema of the underlying data and will make sure that, if something changes in the underlying schema, the ingestion pipeline fails with a clear error.

## Limitations

Hopsworks Feature Store does not support time-travel capabilities for external feature groups. Moreover, as the data resides on external systems, external feature groups cannot be made available online for low latency serving. To make data from an external feature group available online, users need to define an online enabled feature group and hava a job that periodically reads data from the external feature group and writes in the online feature group.

!!! warning "Python support"

    Currently the HSFS library does not support calling the `read()` or `show()` methods on external feature groups. Likewise it is not possibile to call the `read()` or `show()` methods on queries containing external feature groups.
    Nevertheless, external feature groups can be used from a Python engine to create training datasets.

## Creation

{{fg_create}}

## Retrieval

{{fg_get}}

## Properties

{{fg_properties}}

## Methods

{{fg_methods}}
