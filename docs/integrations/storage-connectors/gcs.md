This particular storage connector provides integration to Google Cloud Storage (GCS).
Once you create a storage connector from the FeatureStore UI, you can transact from a GCS bucket into a spark dataframe
by calling the `read` API in FeatureStore.


  <figure>
    <img src="../../../assets/images/storage-connectors/gcs-connector.png" alt="Setup GCS storage connector">
    <figcaption>Configure the GCS storage connector in the Hopsworks UI.</figcaption>
  </figure>

Provide the desired bucket to read from in `GCS Bucket` in the connector UI.

Authentication to GCP is handled by uploading the `JSON keyfile for service account` to the Hopsworks Project. For more information
on service accounts and creating keyfile in GCP, read [Google Cloud documentation.](https://cloud.google.com/docs/authentication/production#create_service_account
'creating service account keyfile')

The connector also supports the optional encryption method `Customer Supplied Encryption Key` by Google.
The encryption details are stored as `Secrets` in the FeatureStore for keeping it secure.
Read more about encryption on [Google Documentation.](https://cloud.google.com/storage/docs/encryption#customer-supplied_encryption_keys)

The storage connector uses the Google `gcs-connector-hadoop` behind the scenes. For more information, check out [Google Cloud Storage Connector for Spark and Hadoop](
https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/gcs#google-cloud-storage-connector-for-spark-and-hadoop 'google-cloud-storage-connector-for-spark-and-hadoop')
