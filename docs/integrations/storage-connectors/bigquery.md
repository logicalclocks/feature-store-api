The BigQuery storage connector provides integration to Google Cloud BigQuery.
Create a connector from the FeatureStore UI, which integrates to run bigquery on your GCP cluster and load results into
spark dataframe by calling the `read` API in FeatureStore.


  <figure>
    <img src="../../../assets/images/storage-connectors/bigq-connector.png" alt="Setup BigQuery storage connector">
    <figcaption>Configure the BigQuery storage connector in the Hopsworks UI.</figcaption>
  </figure>


There are two ways to read via BigQuery, using the **BigQuery Table** or **BigQuery Query** option.
The table option reads directly from the BigQuery table reference. In the UI set the below fields,

* *BigQuery Project*: The BigQuery project
* *BigQuery Dataset*: The dataset of the table
* *BigQuery Table*: The table to read

The second option is to read by passing a SQL query at runtime, by selecting **BigQuery Query** and set,

* *Materiliazation Dataset*: Temporary dataset used by BigQuery

Optionally, you can set any additional spark options using the `Key - Value` options.

Authentication to GCP is handled by uploading the `JSON keyfile for service account` to the Hopsworks Project. For more information
on service accounts and creating keyfile in GCP, read [Google Cloud documentation.](https://cloud.google.com/docs/authentication/production#create_service_account
'creating service account keyfile')

The storage connector uses the Google `spark-bigquery-connector` behind the scenes.
To read more about the spark connector, like the spark options or usage, check [Apache Spark SQL connector for Google BigQuery.](https://github.com/GoogleCloudDataproc/spark-bigquery-connector#usage
'github.com/GoogleCloudDataproc/spark-bigquery-connector')
