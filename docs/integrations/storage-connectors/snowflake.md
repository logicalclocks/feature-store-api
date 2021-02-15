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

* Cluster identifier: The name of the cluster
* Database driver: You can use the default JDBC Snowflake Driver `com.snowflake.jdbc42.Driver` (More on this later)
* Database endpoint: The endpoint for the database. Should be in the format of `[UUID].eu-west-1.snowflake.com`
* Database name: The name of the database to query
* Database port: The port of the cluster. Defaults to 5349


There are two options available for authentication. The first option is to configure a username and a password. The second option is to configure an IAM role. With IAM roles, Jobs or notebooks launched on Hopsworks do not need to explicitly authenticate with Snowflake, as the HSFS library will transparently use the IAM role to acquire a temporary credential to authenticate the specified user.

With regards to the database driver, the library to interact with Snowflake *is not* included in Hopsworks - you need to upload the driver yourself. First, you need to [download the library](https://). You then upload the driver files to the “Resources” dataset in your project, see the screenshot below.

<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/snowflake-add-driver-jupyter.png" alt="When you start a Jupyter notebook, you need to add the driver so it can be accessed in programs.">
    <figcaption>When you start a Jupyter notebook, you need to add the driver so it can be accessed in programs.</figcaption>
  </figure>
</p>

Then, you add the file to your notebook or job before launching it, as shown in the screenshots below.

<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/snowflake-upload-driver.png" alt="Upload the Snowflake the driver.">
    <figcaption>Upload the Snowflake the driver to Hopsworks.</figcaption>
  </figure>
</p>
