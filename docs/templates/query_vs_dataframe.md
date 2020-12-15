# Query vs DataFrame

HSFS provides a DataFrame API to ingest data into the Hopsworks Feature Store. You can also retrieve feature data in a DataFrame, that can either be used directly to train models or [materialized to file(s)](training_dataset.md) for later use to train models.

The idea of the Feature Store is to have pre-computed features available for both training and serving models. The key functionality required to generate training datasets from reusable features are: feature selection, joins, filters and point in time queries. To enable this functionality, we are introducing a new expressive Query abstraction with `HSFS` that provides these operations and guarantees reproducible creation of training datasets from features in the Feature Store.

The new joining functionality is heavily inspired by the APIs used by Pandas to merge DataFrames. The APIs allow you to specify which features to select from which feature group, how to join them and which features to use in join conditions.

```python
# create a query
feature_join = rain_fg.select_all()
                         .join(temperature_fg.select_all(), on=["date", "location_id"])
                         .join(location_fg.select_all())

td = fs.create_training_dataset("rain_dataset",
                          version=1,
                          label=”weekly_rain”,
                          data_format=”tfrecords”)

# materialize query in the specified file format
td.save(feature_join)

# use materialized training dataset for training, possibly in a different environment
td = fs.get_training_dataset(“rain_dataset”, version=1)

# get TFRecordDataset to use in a TensorFlow model
dataset = td.tf_data().tf_record_dataset(batch_size=32, num_epochs=100)

# reproduce query for online feature store and drop label for inference
jdbc_querystring = td.get_query(online=True, with_label=False)
```

If a data scientist wants to modify a new feature that is not available in the Feature Store, she can write code to compute the new feature (using existing features or external data) and ingest the new feature values into the Feature Store. If the new feature is based solely on existing feature values in the Feature Store, we call it a derived feature. The same HSFS APIs can be used to compute derived features as well as features using external data sources.

## The Query Abstraction

Most operations performed on `FeatureGroup` metadata objects will return a `Query` with the applied operation.

### Examples

For example, selecting features from a feature group is a lazy operation, returning a query with the selected
features only:

```python
rain_fg = fs.get_feature_group("rain_fg")

# Returns Query
feature_join = rain_fg.select(["location_id", "weekly_rainfall"])
```

Similarly joins return queries:

```python
feature_join = rain_fg.select_all()
                         .join(temperature_fg.select_all(), on=["date", "location_id"])
                         .join(location_fg.select_all())
```

As well as filters:
```python
feature_join = rain_fg.filter(rain_fg.location_id == 10)
```

## Methods

{{query_methods}}

## Properties

{{query_properties}}
