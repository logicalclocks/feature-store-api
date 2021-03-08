Snowflake is a popular managed data warehouse on AWS, Azure, and GCP.

<!--
<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/snowflake.png" alt="Setup a Snowflake storage connector">
    <figcaption>Configure the Snowflake storage connector in the Hopsworks UI.</figcaption>    
  </figure>
</p>
-->

In the UI for the Snowflake connector, you should enter the following:

The following options are required to create a snowflake connector:

- url: the hostname for your account in the following format: <account_name>.snowflakecomputing.com.
- user: login name for the Snowflake user.
- password: password of the Snowflake user. (required if token is not set)
- token: OAuth token that can be used to access snowflake. (required if password is not set)
- database: the database to use for the session after connecting.
- schema: the schema to use for the session after connecting.

The remaining options are not required, but are optional:

- warehouse: the default virtual warehouse to use for the session after connecting.
- role: the default security role to use for the session after connecting.
- table: the table to which data is written to or read from. 

Additional snowflake options can be added as a list of key-value pair in sfOptions

There are two options available for authentication. The first option is to configure a username and a password. The second option is to use an OAuth token. See [Configure Snowflake OAuth](https://docs.snowflake.com/en/user-guide/oauth-custom.html) for instruction on how to configure OAuth support for snowflake, and [Using External OAuth](https://docs.snowflake.com/en/user-guide/spark-connector-use.html#using-external-oauth) on how you can use External OAuth to authenticate to Snowflake.

With regards to the database driver, the library to interact with Snowflake *is not* included in Hopsworks - you need to upload the driver yourself. First, you need to [download the jdbc driver](https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc) and to use snowflake as the data source in spark [the snowflake spark connector](https://docs.snowflake.com/en/user-guide/spark-connector-install.html).

<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/snowflake-upload-driver.png" alt="Upload the Snowflake the driver.">
    <figcaption>Upload the JDBC driver and Snowflake Spark connector to Hopsworks.</figcaption>
  </figure>
</p>

Then, you add the file to your notebook or job before launching it, as shown in the screenshots below.

<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/snowflake-add-driver-jupyter.png" alt="When you start a Jupyter notebook, you need to add the driver so it can be accessed in programs.">
    <figcaption>When you start a Jupyter notebook for Snowflake, you need to add both the JDBC driver and the Snowflake Spark Connector.</figcaption>
  </figure>
</p>

