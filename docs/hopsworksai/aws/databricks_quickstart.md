# Databricks Quick Start (AWS)

This guide shows how to quickly get started with using the Feature Store from Databricks.
A more complete guide is available: [databricks integration guide](../../integrations/databricks.md).

## Step 1: Deploying Hopsworks to the Databricks VPC

The easiest way to connect to the Feature Store from Databricks is to deploy Hopsworks to the same
VPC as your Databricks clusters. For other options, including VPC peering, see [databricks integration guide](../../integrations/databricks.md).

### Step 1.3: Deploying a new Hopsworks clusters

In Hopsworks.ai, select *Create cluster*:

<p align="center">
  <figure>
    <a  href="../../../assets/images/hopsworksai/create-instance.png">
      <img src="../../../assets/images/hopsworksai/create-instance.png" alt="Create a Hopsworks cluster">
    </a>
    <figcaption>Create a Hopsworks cluster</figcaption>
  </figure>
</p>

As region, choose the region you use for Databricks:

.. _configure_databricks_step_1.png: ../../../_images/configure_databricks_step_1.png
.. figure:: ../../../imgs/hopsworksai/configure_databricks_step_1.png
    :alt: Deploying Hopsworks in the Databricks VPC
    :target: `configure_databricks_step_1.png`_
    :align: center
    :figclass: align-center

{!hopsworksai/aws/s3_permissions.md!}

Select the VPC with a name starting with *databricks-*:

.. _configure_databricks_step_2.png: ../../../_images/configure_databricks_step_2.png
.. figure:: ../../../imgs/hopsworksai/configure_databricks_step_2.png
    :alt: Deploying Hopsworks in the Databricks VPC
    :target: `configure_databricks_step_2.png`_
    :align: center
    :figclass: align-center

Finally, select the subnet in the *Availability Zone* of your Databricks cluster and create the cluster:

.. _configure_databricks_step_3.png: ../../../_images/configure_databricks_step_3.png
.. figure:: ../../../imgs/hopsworksai/configure_databricks_step_3.png
    :alt: Deploying Hopsworks in the Databricks VPC
    :target: `configure_databricks_step_3.png`_
    :align: center
    :figclass: align-center

Wait for the cluster to start. After starting, select *Feature Store* and *Online Feature Store* under *Exposed Services*
and press *Update*. This will update the *Security Group* attached to the Hopsworks cluster to allow incoming traffic on the relevant ports.

.. _open-ports.png: ../../../_images/open-ports.png
.. figure:: ../../../imgs/hopsworksai/open-ports.png
    :alt: Outside Access to the Feature Store
    :target: `open-ports.png`_
    :align: center
    :figclass: align-center

You can now log in to Hopsworks with the given link and credentials:

.. _credentials.png: ../../../_images/credentials.png
.. figure:: ../../../imgs/hopsworksai/credentials.png
    :alt: Hopsworks Access
    :target: `credentials.png`_
    :align: center
    :figclass: align-center

.. include:: ../../../featurestore/integrations/guides/databricks.rst
  :start-after: .. include-2-start
  :end-before:  .. include-2-stop

## Step 3: Storing the API Key in the AWS Secrets Manager

.. include:: ../../../featurestore/integrations/guides/databricks.rst
  :start-after: .. include-3-start
  :end-before:  .. include-3-stop

## Step 4: Connecting to the Feature Store from Databricks

### Step 4.1: Installing the hops SDK

.. include:: ../../../featurestore/integrations/guides/databricks.rst
  :start-after: .. include-4-start
  :end-before:  .. include-4-stop

### Step 4.2: Configuring Databricks to use the Feature Store

After installing the hops library, restart the cluster and open a Databricks notebook connected to this cluster.
Execute the following statements in this notebook:

```python
import hops.featurestore as fs
fs.setup_databricks(
    'my_instance.aws.hopsworks.ai', # Hopsworks.ai address of your Feature Store instance
    'my_project',                   # Name of your Hopsworks Feature Store project
    region_name='my_aws_region',    # AWS region in which you stored the API Key
    secrets_store='secretsmanager', # Either parameterstore or secretsmanager
    hostname_verification=True)     # Disable for self-signed certificates
```

**This will return two configurations that you need to add to your Databricks cluster configuration.**

.. include:: ../../../featurestore/integrations/guides/databricks.rst
  :start-after: .. include-5-start
  :end-before:  .. include-5-stop

### Step 4.3: Connecting to the Feature Store

.. include:: ../../../featurestore/integrations/guides/databricks.rst
  :start-after: .. include-6-start
  :end-before:  .. include-6-stop

```python
import hops.featurestore as fs
fs.connect(
    'my_instance.aws.hopsworks.ai', # Hopsworks.ai address of your Feature Store instance
    'my_project',                   # Name of your Hopsworks Feature Store project
    region_name='my_aws_region',    # AWS region in which you stored the API Key
    secrets_store='secretsmanager', # Either parameterstore or secretsmanager
    hostname_verification=True)     # Disable for self-signed certificates
```

## Step 3: Next steps

Check out our other guides for how to get started with Hopsworks and the Feature Store:

* `Feature Store Quick Start notebook <https://github.com/logicalclocks/hops-examples/blob/master/notebooks/featurestore/databricks/FeatureStoreQuickStartDatabricks.ipynb>`_
* `Feature Store Tour notebook <https://github.com/logicalclocks/hops-examples/blob/master/notebooks/featurestore/FeaturestoreTourPython.ipynb>`_
* Get started with the [Hopsworks Feature Store](../../quickstart.md)
* Get started with Machine Learning on Hopsworks: [HopsML](https://hopsworks.readthedocs.io/en/stable/hopsml/index.html#hops-ml)
* Get started with Hopsworks: [User Guide](https://hopsworks.readthedocs.io/en/stable/user_guide/user_guide.html#userguide)
* Code examples and notebooks: [hops-examples](https://github.com/logicalclocks/hops-examples)
