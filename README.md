Hopsworks Feature Store API
===========================
![Discourse users](https://img.shields.io/discourse/users?label=Hopsworks%20Community&server=https%3A%2F%2Fcommunity.hopsworks.ai)
[![Docs](https://img.shields.io/badge/docs-hsfs-orange)](https://docs.hopsworks.ai/)
[![PypiStatus](https://img.shields.io/pypi/v/hsfs?color=blue)](https://pypi.org/project/hsfs/)
[![Java](https://img.shields.io/badge/java-HSFS-green)](https://archiva.hops.works/#artifact/com.logicalclocks/hsfs)
[![Downloads](https://pepy.tech/badge/hsfs/month)](https://pepy.tech/project/hsfs/month)
[![CodeStyle](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
![PyPI - License](https://img.shields.io/pypi/l/hsfs?color=green)

HSFS is the library to interact with the Hopsworks Feature Store. The library makes creating new features, feature groups and training datasets easy.

The library is environment indpendent and can be used in two modes:

- Spark mode: For data engineering jobs that create and write features into the feature store or generate training datasets. It requires a Spark environment such as the one provided in the Hopsworks platform or Databricks. In Spark mode, HSFS provides bindings both for Python and JVM languages.

- Python mode: For data science jobs to explore the features available in the feature store, generate training datasets and feed them in a training pipeline. Python mode requires just a Python interpreter and can be used both in Hopsworks from Python Jobs/Jupyter Kernels, Amazon SageMaker or KubeFlow.

The library automatically configures itself based on the environment it is run.
However, to connect from an external environment such as Databricks or AWS Sagemaker,
additional connection information, such as host and port, is required. For more information about the setup from external environments, see the setup section.

Getting Started On Hopsworks
---------------

Instantiate a connection and get the project feature store handler
=== "Python"

    ```python
    import hsfs

    connection = hsfs.connection()
    fs = connection.get_feature_store()
    ```

=== "Java"

    ```java
    import com.logicalclocks.hsfs._

    val connection = HopsworksConnection.builder().build();
    val fs = connection.getFeatureStore();
    ```


Create a new feature group
```python
fg = fs.create_feature_group("rain",
                        version=1,
                        description="Rain features",
                        primary_key=['date', 'location_id'],
                        online_enabled=True)

fg.save(dataframe)
```

Join features together
```python
feature_join = rain_fg.select_all()
                    .join(temperature_fg.select_all(), ["date", "location_id"])
                    .join(location_fg.select_all()))

feature_join.show(5)
```

Use the query object to create a training dataset:
```python
td = fs.create_training_dataset("training_dataset",
                           version=1,
                           data_format="tfrecords",
                           description="A test training dataset saved in TfRecords format",
                           splits={'train': 0.7, 'test': 0.2, 'validate': 0.1})

td.save(feature_join)
```

Feed the training dataset to a TensorFlow model:
```python
train_input_feeder = training_dataset.feed(target_name='label',split='train', is_training=True)
train_input = train_input_feeder.tf_record_dataset()
```

You can find more examples on how to use the library in our [hops-examples](https://github.com/logicalclocks/hops-examples) repository.

Issues
------

Please report any issue using [Github issue tracking](https://github.com/logicalclocks/feature-store-api/issues)
