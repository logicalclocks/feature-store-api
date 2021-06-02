# Hopsworks.ai API Key

Hopsworks.ai allows users to generate an API Key that can be used to authenticate and access the Hopsworks.ai REST APIs.

## Generate an API Key

First, login to your Hopsworks.ai account, then click on the Settings tab as shown below:

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/apiKey/api-key-1.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/apiKey/api-key-1.png" alt="Click on Settings">
    </a>
    <figcaption>Click on the Settings tab</figcaption>
  </figure>
</p>

Click on the API Key tab, and then click on the *Generate API Key* button:

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/apiKey/api-key-2.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/apiKey/api-key-2.png" alt="Generate API Key">
    </a>
    <figcaption>Generate an API Key</figcaption>
  </figure>
</p>

Copy the generated API Key and store it in a secure location.

!!! warning
    Make sure to copy your API Key now. You won’t be able to see it again. However, you can always delete it and generate a new one.


<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/apiKey/api-key-3.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/apiKey/api-key-3.png" alt="Copy API Key">
    </a>
    <figcaption>Copy the generated API Key</figcaption>
  </figure>
</p>

## Use the API Key

To access the Hopsworks.ai REST APIs, you should pass the API key as a header **x-api-key** when executing requests on Hopsworks.ai as shown below:

```bash
curl -XGET -H "x-api-key: <YOUR API KEY>" https://api.hopsworks.ai/api/clusters
```

Alternatively, you can use your API Key with the [Hopsworks.ai terraform provider](https://registry.terraform.io/providers/logicalclocks/hopsworksai/latest) to manage your Hopsworks clusters using [terraform](https://www.terraform.io/).

## Delete your API Key

First, login to your Hopsworks.ai account, click on the Settings tab, then click on the API Key tab, and finally click on *Delete API Key* as shown below:

<p align="center">
  <figure>
    <a  href="../../assets/images/hopsworksai/apiKey/api-key-4.png">
      <img style="border: 1px solid #000" src="../../assets/images/hopsworksai/apiKey/api-key-4.png" alt="Delete API Key">
    </a>
    <figcaption>Delete your API Key</figcaption>
  </figure>
</p>
 
