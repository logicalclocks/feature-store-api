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

* "url": "https://12345.west-europe.azure.snowflakecomputing.com", # required
* "user": "HOPSWORKS", # required
* "password": "snowflake password", # required if no token
* "token": "oauth token", # required if no password
* "database": "ML_WORKSHOP", # required
* "schema": "PUBLIC", # required

The following options are not required (optional):

* warehouse: the default virtual warehouse to use for the session after connecting
* role: the default security role to use for the session after connecting
* table: the table to which data is written to or read from. 

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

