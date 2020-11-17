SageMaker Quick Start
=====================

.. contents:: :local:

Step 1: Deploying Hopsworks
---------------------------

In Hopsworks.ai, select *Create cluster*:

.. _create-instance.png: ../../../_images/create-instance.png
.. figure:: ../../../imgs/hopsworksai/create-instance.png
    :alt: Create a Hopsworks cluster
    :target: `create-instance.png`_
    :align: center
    :figclass: align-center

Select the region of your SageMaker instance as region and create the cluster.

.. note::

  If you choose to use an existing VPC, then use the same VPC and subnet as for your SageMaker instance.

.. _configure_databricks_step_1.png: ../../../_images/configure_databricks_step_1.png
.. figure:: ../../../imgs/hopsworksai/configure_databricks_step_1.png
    :alt: Configuring the Hopsworks cluster
    :target: `configure_databricks_step_1.png`_
    :align: center
    :figclass: align-center

.. include:: ../../../getting_started/hopsworksai/guides/s3_permissions.rst

By default, only the Hopsworks UI is exposed (made available to clients on external networks, like the Internet)
by your Hopsworks cluster. To reach the Feature Store from SageMaker, you have
to expose it. Under *Exposed services*, select *Feature Store* and *Online Feature Store* and press *Update*.
This will update the *Security Group* attached to the Hopsworks cluster to allow incoming traffic on the relevant ports.

.. _open-ports.png: ../../../_images/open-ports.png
.. figure:: ../../../imgs/hopsworksai/open-ports.png
    :alt: Outside Access to the Feature Store
    :target: `open-ports.png`_
    :align: center
    :figclass: align-center

Use the provided link and credentials to log in to your Hopsworks cluster.

.. _credentials.png: ../../../_images/credentials.png
.. figure:: ../../../imgs/hopsworksai/credentials.png
    :alt: Hopsworks Access
    :target: `credentials.png`_
    :align: center
    :figclass: align-center


Step 2: Generating an API key
-----------------------------

.. include:: ../../../featurestore/integrations/guides/sagemaker.rst
  :start-after: .. include-1-start
  :end-before:  .. include-1-stop

Step 3: Storing the API Key in the AWS Secrets Manager
------------------------------------------------------

.. include:: ../../../featurestore/integrations/guides/sagemaker.rst
  :start-after: .. include-2.1-start
  :end-before:  .. include-2.1-stop

.. include:: ../../../featurestore/integrations/guides/sagemaker.rst
  :start-after: .. include-2-start
  :end-before:  .. include-2-stop

Step 4: Installing hopsworks-cloud-sdk
--------------------------------------

.. include:: ../../../featurestore/integrations/guides/sagemaker.rst
  :start-after: .. include-3-start
  :end-before:  .. include-3-stop

Step 5: Connecting to the Feature Store
---------------------------------------

.. include:: ../../../featurestore/integrations/guides/sagemaker.rst
  :start-after: .. include-4-start
  :end-before:  .. include-4-stop

Step 6: Next steps
------------------

Check out our other guides for how to get started with Hopsworks and the Feature Store:

.. hlist:

* `Feature Store Tour Notebook <https://github.com/logicalclocks/hops-examples/blob/master/notebooks/featurestore/aws/SageMakerFeaturestoreTourPython.ipynb>`_
* Get started with the :ref:`feature-store`
* Get started with Machine Learning on Hopsworks: :ref:`hops-ml`
* Get started with Hopsworks: :ref:`userguide`
* Code examples and notebooks: `hops-examples <https://github.com/logicalclocks/hops-examples>`_
