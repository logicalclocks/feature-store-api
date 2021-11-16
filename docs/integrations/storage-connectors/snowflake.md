Snowflake is a popular cloud-native data warehouse service, and supports scalable feature computation with SQL. However, Snowflake is not viable as an online feature store that serves features to models in production, with its columnar database layout its latency is too high compared to OLTP databases or key-value stores.

To interact with Snowflake and to register and read [external feature groups](../../../generated/on_demand_feature_group) users need to define a storage connector using the UI:

<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/snowflake.png" alt="Snowflake connector UI">
    <figcaption>Snowflake connector UI</figcaption>
  </figure>
</p>

To configure the connector users need to provide the `Connection URL` of the cluster.

The Snowflake storage connector supports both username and password authentication as well as token-based authentication.

!!! warning "Token-based authentication Beta"

    Currently token-based authentication is in beta phase. Users are advised to use username/password and/or create a service account for accessing Snowflake from Hopsworks.


The Hopsworks Snowflake storage connector allows users to specify several additional fields, though only two are mandatory: the database field and the schema field.

The role field can be used to specify which [Snowflake security role](https://docs.snowflake.com/en/user-guide/security-access-control-overview.html#system-defined-roles) to assume for the session after the connection is established.

The application field can also be specified to have better observability in Snowflake with regards to which application is running which query. The application field can be a simple string like “Hopsworks” or, for instance, the project name, to track usage and queries from each Hopsworks project.

Additional key/value options can also be specified to control the behavior of the Snowflake Spark connector. The available options are listed in the [Snowflake documentation](https://docs.snowflake.com/en/user-guide/spark-connector-use.html).
