# On-Demand (External) Feature Groups

On-demand (External) Feature Groups are Feature Groups for which the data is stored on an external storage system (e.g. Data Warehouse, S3, ADLS).
From an API prospective, on-demand feature groups can be used in the same way as cached feature groups. Users can pick features from on-demand feature groups to create training datasets. On-demand feature groups can be also used as data source to create derived features, meaning feature on which additional feature engineering is applied.

On-demand feature groups rely on [Storage Connectors](../../integrations/storage-connectors/) to identify the location and authenticate with the external storage.
When the on-demand feature group is define on top of an external database capabale of running SQL statements (i.e. when using the JDBC, Redshift or Snowflake connectors) the on-demand feature group needs to be defined as a SQL statement. SQL statements can contain feature engineering transformations, when reading the on-demand feature group, the SQL statement is pushed down to the storage which executes it.

=== "Python"

    !!! example "Define a SQL based on-demand feature group"
        ```python
        # Retrieve the storage connector defined before
        redshift_conn = fs.get_storage_connector("telco_redshift_cluster")
        telco_on_dmd = fs.create_on_demand_feature_group(name="telco_redshift",
                                                version=1,
                                                query="select * from telco",
                                                description="On-demand feature group for telecom customer data",
                                                storage_connector=redshift_conn,
                                                statistics_config=True)
        telco_on_dmd.save()
        ```

=== "Scala"

    !!! example "Connecting from Hopsworks"
        ```scala
        val redshiftConn = fs.getRedshiftConnector("telco_redshift_cluster")
        val telcoOnDmd = (fs.createOnDemandFeatureGroup()
                    .name("telco_redshift_scala")
                    .version(2)
                    .query("select * from telco")
                    .description("On-demand feature group for telecom customer data")
                    .storageConnector(redshiftConn)
                    .statisticsEnabled(true)
                    .build())
        telcoOnDmd.save()
        ```


When defining an on-demand feature group on top of a object store/external filesystem (i.e. when using the S3 or the ADLS connector) the underlying data is required to have a schema. The underlying data can be stored in ORC, Parquet, Delta, Hudi or Avro, and the schema for the feature group will be extracted by the files metadata.

=== "Python"

    !!! example "Define a SQL based on-demand feature group"
        ```python
        # Retrieve the storage connector defined before
        s3_conn = fs.get_storage_connector("telco_s3_bucket")
        telco_on_dmd = fs.create_on_demand_feature_group(name="telco_s3",
                                                version=1,
                                                data_format="parquet",
                                                description="On-demand feature group for telecom customer data",
                                                storage_connector=s3_conn,
                                                statistics_config=True)
        telco_on_dmd.save()
        ```

=== "Scala"

    !!! example "Connecting from Hopsworks"
        ```scala
        val s3Conn = fs.getS3Connector("telco_s3_bucket")
        val telcoOnDmd = (fs.createOnDemandFeatureGroup()
                    .name("telco_s3")
                    .version(1)
                    .dataFormat(OnDemandDataFormat.PARQUET)
                    .description("On-demand feature group for telecom customer data")
                    .storageConnector(s3Conn)
                    .statisticsEnabled(true)
                    .build())
        telcoOnDmd.save()
        ```

## Use cases

There are two use cases in which a user can benefit from on-demand feature groups:

- **Existing feature engineering pipelines**: in case users have recently migrated to Hopsworks Feature Store and they have existing feature engineering pipelines in production. Users can register the output of the existing pipelines as on-demand feature groups in Hopsworks, and immediately use their features to build training datasets. With on-demand feature groups, users do not have to modify the existing pipelines to write to the Hopsworks Feature Store.

- **Data Ingestion**: on-demand feature groups can be used as a data source. The benefit of using on-demand feature groups to ingest data from external sources is that the Hopsworks Feature Store keeps track of where the data is located and how to authenticate with the external storage system. In addition to that, the Hopsworks Feature Store tracks also the schema of the underlying data will make sure that, if something changes in the underlying schema, the ingestion pipline fails with a clear error.

## Limitations

Hopsworks Feature Store does not support time-travel capabilities for on-demand feature groups. Moreover, as the data resides on external systems, on-demand feature groups cannot be made available online for low latency serving. To make data from an on-demand feature group available online, users need to define an online enabled feature group and hava a job that periodically reads data from the on-demand feature group and writes in the online feature group.

!!! warning "Python support"

    Currently the HSFS library does not support calling the `read()` or `show()` methods on on-demand feature groups. Likewise it is not possibile to call the `read()` or `show()` methods on queries containing on-demand feature groups.
    Nevertheless, on-demand feature groups can be used from a Python engine to create training datasets.

## Creation

{{fg_create}}

## Retrieval

{{fg_get}}

## Properties

{{fg_properties}}

## Methods

{{fg_methods}}
