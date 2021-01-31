## ADLS
Azure Data Lake Storage (ADLS) Gen2 is a HDFS-compatible filesystem on Azure for data analytics. The ADLS Gen2 filesystem stores its data in Azure Blob storage, ensuring low-cost storage, high availability, and disaster recovery. In Hopsworks, you can access ADLS Gen2 by defining a Storage Connector with a service principal.

### Requirements

* [Create an Azure Data Lake Storage Gen2 account](https://docs.microsoft.com/azure/storage/data-lake-storage/quickstart-create-account) and (initialize a filesystem, enabling the hierarchical namespace)[https://docs.microsoft.com/azure/storage/data-lake-storage/namespace]. Note that your storage account must belong to an Azure resource group.

* [Create an Azure AD application and service principal](https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal) that can access your ADLS storage account.
* Register the service principal, granting the correct role assignment, such as Storage Blob Data Contributor, on the Azure Data Lake Storage Gen2 account.

!!! info
    When you specify the 'container name' in the ADLS storage connector, you need to have previously created that container - the Hopsworks Feature Store will not create that storage container for you.


<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/adls.png" alt="Define a Azure Data Lake storage connector using service credentials and account details.">
    <figcaption>You can define a Azure Data Lake Storage (ADLS) connector using the account details (managed identity, application id) and a service credential.</figcaption>
  </figure>
</p>


## Azure Create a ADLS Resource


When programmatically signing in, you need to pass the tenant ID with your authentication request and the application ID. You also need a certificate or an authentication key (described in 
the following section). To get those values, use the following steps:

1. Select Azure Active Directory.

2. From App registrations in Azure AD, select your application.

3. Copy the Directory (tenant) ID and store it in your application code.

<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/adls-copy-tenant-id.png" alt="ADLS select tenant-id.">
    <figcaption>You need to copy the Directory (tenant) id and paste it to the Hopsworks ADLS storage connector  "Directory id" text field.</figcaption>
  </figure>
</p>

4. Copy the Application ID and store it in your application code.
<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/adls-copy-app-id.png" alt="ADLS select app-id.">
    <figcaption>You need to copy the Application id and paste it to the Hopsworks ADLS storage connector "Application id" text field.</figcaption>
  </figure>
</p>


5. Create an Application Secret and copy it into the Service Credential field.

<p align="center">
  <figure>
    <img src="../../../assets/images/storage-connectors/adls-copy-secret.png" alt="ADLS enter application secret.">
    <figcaption>You need to copy the Application Secret and paste it to the Hopsworks ADLS storage connector "Service Credential" text field.</figcaption>
  </figure>
</p>


References: 
  https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal

