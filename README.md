# Hopsworks Feature Store

<p align="center">
  <a href="https://community.hopsworks.ai"><img
    src="https://img.shields.io/discourse/users?label=Hopsworks%20Community&server=https%3A%2F%2Fcommunity.hopsworks.ai"
    alt="Hopsworks Community"
  /></a>
    <a href="https://docs.hopsworks.ai"><img
    src="https://img.shields.io/badge/docs-HSFS-orange"
    alt="Hopsworks Feature Store Documentation"
  /></a>
  <a href="https://pypi.org/project/hsfs/"><img
    src="https://img.shields.io/pypi/v/hsfs?color=blue"
    alt="PyPiStatus"
  /></a>
  <a href="https://archiva.hops.works/#artifact/com.logicalclocks/hsfs"><img
    src="https://img.shields.io/badge/java-HSFS-green"
    alt="Scala/Java Artifacts"
  /></a>
  <a href="https://pepy.tech/project/hsfs/month"><img
    src="https://pepy.tech/badge/hsfs/month"
    alt="Downloads"
  /></a>
  <a href="https://github.com/psf/black"><img
    src="https://img.shields.io/badge/code%20style-black-000000.svg"
    alt="CodeStyle"
  /></a>
  <a><img
    src="https://img.shields.io/pypi/l/hsfs?color=green"
    alt="License"
  /></a>
</p>

HSFS is the library to interact with the Hopsworks Feature Store. The library makes creating new features, feature groups and training datasets easy.

The library is environment independent and can be used in two modes:

- Spark mode: For data engineering jobs that create and write features into the feature store or generate training datasets. It requires a Spark environment such as the one provided in the Hopsworks platform or Databricks. In Spark mode, HSFS provides bindings both for Python and JVM languages.

- Python mode: For data science jobs to explore the features available in the feature store, generate training datasets and feed them in a training pipeline. Python mode requires just a Python interpreter and can be used both in Hopsworks from Python Jobs/Jupyter Kernels, Amazon SageMaker or KubeFlow.

The library automatically configures itself based on the environment it is run.
However, to connect from an external environment such as Databricks or AWS Sagemaker,
additional connection information, such as host and port, is required. For more information about the setup from external environments, see the setup section.

## Getting Started On Hopsworks

Instantiate a connection and get the project feature store handler
```python
import hsfs

connection = hsfs.connection()
fs = connection.get_feature_store()
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

Upsert new data in to the feature group with `time_travel_format="HUDI"`".
```python
fg.insert(upsert_df)
```

Retrieve commit timeline metdata of the feature group with `time_travel_format="HUDI"`".
```python
fg.commit_details()
```

"Reading feature group as of specific point in time".
```python
fg = fs.get_feature_group("rain", 1)
fg.read("2020-10-20 07:34:11").show()
```

Read updates  that occurred between specified points in time.
```python
fg = fs.get_feature_group("rain", 1)
fg.read_changes("2020-10-20 07:31:38", "2020-10-20 07:34:11").show()
```

Join features together
```python
feature_join = rain_fg.select_all()
                    .join(temperature_fg.select_all(), on=["date", "location_id"])
                    .join(location_fg.select_all())
feature_join.show(5)
```

join feature groups that correspond to specific point in time
```python
feature_join = rain_fg.select_all()
                    .join(temperature_fg.select_all(), on=["date", "location_id"])
                    .join(location_fg.select_all())
                    .as_of("2020-10-31")
feature_join.show(5)
```

join feature groups that correspond to different time
```python
rain_fg_q = rain_fg.select_all().as_of("2020-10-20 07:41:43")
temperature_fg_q = temperature_fg.select_all().as_of("2020-10-20 07:32:33")
location_fg_q = location_fg.select_all().as_of("2020-10-20 07:33:08")
joined_features_q = rain_fg_q.join(temperature_fg_q).join(location_fg_q)
```

Use the query object to create a training dataset:
```python
td = fs.create_training_dataset("rain_dataset",
                                version=1,
                                data_format="tfrecords",
                                description="A test training dataset saved in TfRecords format",
                                splits={'train': 0.7, 'test': 0.2, 'validate': 0.1})

td.save(feature_join)
```

A short introduction to the Scala API:
```scala
import com.logicalclocks.hsfs._
val connection = HopsworksConnection.builder().build()
val fs = connection.getFeatureStore();
val attendances_features_fg = fs.getFeatureGroup("games_features", 1);
attendances_features_fg.show(1)
```

You can find more examples on how to use the library in our [hops-examples](https://github.com/logicalclocks/hops-examples) repository.

## Documentation

Documentation is available at [Hopsworks Feature Store Documentation](https://docs.hopsworks.ai/).

## Issues

For general questions about the usage of Hopsworks and the Feature Store please open a topic on [Hopsworks Community](https://community.hopsworks.ai/).

Please report any issue using [Github issue tracking](https://github.com/logicalclocks/feature-store-api/issues).


## Contributing

If you would like to contribute to this library, please see the [Contribution Guidelines](CONTRIBUTING.md).
