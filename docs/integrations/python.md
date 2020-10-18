# Python Environments (Local or Kubeflow)

Connecting to the Feature Store from any Python environment requires setting up a Feature Store API Key and installing the library. This guide explains step by step how to connect to the Feature Store from any Python environment such as your local environment or KubeFlow.

## Generating an API Key

In Hopsworks, click on your *username* in the top-right corner and select *Settings* to open the user settings. Select *Api keys*. Give the key a name and select the job, featurestore and project scopes before creating the key. Copy the key into your clipboard.

Create a file called `featurestore.key` in your designated Python environment and save the API Key from your clipboard in the file.

!!! success "Scopes"
    The created API-Key should at least have the following scopes:

    1. featurestore
    2. project
    3. job

<p align="center">
  <figure>
    <img src="../../assets/images/api-key.png" alt="Generating an API Key on Hopsworks">
    <figcaption>API-Keys can be generated in the User Settings on Hopsworks</figcaption>
  </figure>
</p>

!!! info
    You are only ably to retrieve the API Key once. If you miss to copy it to your clipboard, delete it again and create a new one.

## Installing **HSFS**

To be able to access the Hopsworks Feature Store, the `HSFS` Python library needs to be installed in the environment from which you want to connect to the Feature Store. You can install the library through pip. We recommend using a Python environment manager such as *virtualenv* or *conda*.

```
pip install hsfs~=[HOPSWORKS_VERSION]
```

!!! attention "Matching Hopsworks version"
    The **major version of `HSFS`** needs to match the **major version of Hopsworks**.


<p align="center">
    <figure>
        <img src="../../assets/images/hopsworks-version.png" alt="HSFS version needs to match the major version of Hopsworks">
        <figcaption>You find the Hopsworks version inside any of your Project's settings tab on Hopsworks</figcaption>
    </figure>
</p>

## Connecting to the Feature Store

You are now ready to connect to the Hopsworks Feature Store from your Python environment:

```python
import hsfs
conn = hsfs.connection(
    'my_instance',                      # DNS of your Feature Store instance
    443,                                # Port to reach your Hopsworks instance, defaults to 443
    'my_project',                       # Name of your Hopsworks Feature Store project
    secrets_store='local',              # local file system
    api_key_file='featurestore.key',    # Path to the previously created file containing the API Key
    hostname_verification=True)         # Disable for self-signed certificates
)
fs = conn.get_feature_store()           # Get the project's default feature store
```

!!! info "Ports"

    If you have trouble connecting, please ensure that your Feature Store can receive incoming traffic from your Python environment on ports 443, 9083 and 9085.

## Next Steps

For more information about how to connect, see the [Connection](../concepts/project.md) guide. Or continue with the Data Source guide to import your own data to the Feature Store.
