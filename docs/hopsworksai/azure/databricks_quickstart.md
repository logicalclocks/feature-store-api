Databricks Quick Start (Azure)
==============================

This guide shows how to quickly get started with using the Feature Store from Databricks on Azure.

.. contents:: :local:

Step 1: Setting up VNet peering between Hopsworks and Databricks
---------------------------------------------------------------
VNet peering between the Hopsworks and the Databricks virtual network is required to be able to connect
to the Feature Store from Databricks.

In the Azure portal, go to Azure Databricks and go to *Virtual Network Peering*:

.. _azure-databricks-7.png: ../../../_images/azure-databricks-7.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-7.png
    :alt: Azure Databricks
    :target: `azure-databricks-7.png`_
    :align: center
    :figclass: align-center

Select *Add Peering*:

.. _azure-databricks-8.png: ../../../_images/azure-databricks-8.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-8.png
    :alt: Add peering
    :target: `azure-databricks-8.png`_
    :align: center
    :figclass: align-center

Name the peering and select the virtual network used by your Hopsworks cluster. The virtual network
is shown in the cluster details on Hopsworks.ai (see the next picture). Ensure to press the copy button
on the bottom of the page and save the value somewhere. Press *Add* and create the peering:

.. _azure-databricks-10.png: ../../../_images/azure-databricks-10.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-10.png
    :alt: Configure peering
    :target: `azure-databricks-10.png`_
    :align: center
    :figclass: align-center

The virtual network used by your cluster is shown under *Details*:

.. _azure-databricks-9.png: ../../../_images/azure-databricks-9.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-9.png
    :alt: Check the Hopsworks virtual network
    :target: `azure-databricks-9.png`_
    :align: center
    :figclass: align-center

The peering connection should now be listed as initiated:

.. _azure-databricks-11.png: ../../../_images/azure-databricks-11.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-11.png
    :alt: Peering connection initiated
    :target: `azure-databricks-11.png`_
    :align: center
    :figclass: align-center

On the Azure portal, go to *Virtual networks* and search for the virtual network used by your
Hopsworks cluster:

.. _azure-databricks-12.png: ../../../_images/azure-databricks-12.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-12.png
    :alt: Virtual networks
    :target: `azure-databricks-12.png`_
    :align: center
    :figclass: align-center

Open the network and select *Peerings*:

.. _azure-databricks-13.png: ../../../_images/azure-databricks-13.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-13.png
    :alt: Select peerings
    :target: `azure-databricks-13.png`_
    :align: center
    :figclass: align-center

Choose to add a peering connection:

.. _azure-databricks-14.png: ../../../_images/azure-databricks-14.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-14.png
    :alt: Add a peering connection
    :target: `azure-databricks-14.png`_
    :align: center
    :figclass: align-center

Name the peering connection and select *I know my resource ID*. Paste the string copied when creating
the peering from Databricks Azure. If you haven't copied that string, then manually select the virtual
network used by Databricks and press *OK* to create the peering:

.. _azure-databricks-15.png: ../../../_images/azure-databricks-15.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-15.png
    :alt: Configure peering
    :target: `azure-databricks-15.png`_
    :align: center
    :figclass: align-center

The peering should now be *Updating*:

.. _azure-databricks-16.png: ../../../_images/azure-databricks-16.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-16.png
    :alt: Cloud account settings
    :target: `azure-databricks-16.png`_
    :align: center
    :figclass: align-center

Wait for the peering to show up as *Connected*. There should now be bi-directional network connectivity between the Feature Store and Databricks:

.. _azure-databricks-17.png: ../../../_images/azure-databricks-17.png
.. figure:: ../../../imgs/hopsworksai/azure-databricks-17.png
    :alt: Cloud account settings
    :target: `azure-databricks-17.png`_
    :align: center
    :figclass: align-center

.. include:: ../../../featurestore/integrations/guides/databricks.rst
  :start-after: .. include-2-start
  :end-before:  .. include-2-stop

Step 3: Connecting to the Feature Store from Databricks
-------------------------------------------------------

Step 3.1: Installing the hops library
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: ../../../featurestore/integrations/guides/databricks.rst
  :start-after: .. include-4-start
  :end-before:  .. include-4-stop

Step 3.2: Configuring Databricks to use the Feature Store
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After installing the hops library, restart the cluster and open a Databricks notebook connected to this cluster.
Execute the following statements in this notebook.

Ensure to replace YOUR_API_KEY_STRING with the API key created in step 2:

.. code-block:: python

 import hops.featurestore as fs

 # Persist the API key on dbfs
 f = open("/dbfs/fs_apikey.txt", "w")
 f.write("YOUR_API_KEY_STRING")
 f.close()

 fs.setup_databricks(
    'my_instance.cloud.hopsworks.ai',   # Hopsworks.ai address of your Feature Store instance
    'my_project',                       # Name of your Hopsworks Feature Store project
    secrets_store='local',
    api_key_file='/dbfs/fs_apikey.txt', # This should point to a path in you Databricks cluster
    hostname_verification=True)         # Disable for self-signed certificates

**This will return two configurations that you need to add to your Databricks cluster configuration.**

.. include:: ../../../featurestore/integrations/guides/databricks.rst
  :start-after: .. include-5-start
  :end-before:  .. include-5-stop

Step 3.3: Connecting to the Feature Store
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. include:: ../../../featurestore/integrations/guides/databricks.rst
  :start-after: .. include-6-start
  :end-before:  .. include-6-stop

.. code-block:: python

 import hops.featurestore as fs
 fs.connect(
    'my_instance.aws.hopsworks.ai',     # Hopsworks.ai address of your Feature Store instance
    'my_project',                       # Name of your Hopsworks Feature Store project
    secrets_store='local',
    api_key_file='/dbfs/fs_apikey.txt', # This should point to a path in you Databricks cluster
    hostname_verification=True)         # Disable for self-signed certificates

Step 4: Next steps
------------------
Check out our other guides for how to get started with Hopsworks and the Feature Store:

.. hlist:

* `Feature Store Quick Start notebook <https://github.com/logicalclocks/hops-examples/blob/master/notebooks/featurestore/databricks/FeatureStoreQuickStartDatabricks.ipynb>`_
* `Feature Store Tour notebook <https://github.com/logicalclocks/hops-examples/blob/master/notebooks/featurestore/FeaturestoreTourPython.ipynb>`_
* Get started with the :ref:`feature-store`
* Get started with Machine Learning on Hopsworks: :ref:`hops-ml`
* Get started with Hopsworks: :ref:`userguide`
* Code examples and notebooks: `hops-examples <https://github.com/logicalclocks/hops-examples>`_
