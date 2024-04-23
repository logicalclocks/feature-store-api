# Feature Store API Quick Tour

The `FeatureStore` serves as the central repository for features data and metadata in the Hopsworks Platform.

## Get or Create Feature Groups to write to the Feature Store

You can retrieve or create `FeatureGroup` objects using the `get_or_create_feature_group()` method. `FeatureGroup`s objects encapsulate metadata about a table in the Feature Store, such as its schema, and are used to insert new data in the `FeatureStore`.

```python
my_feature_group = fs.get_or_create_feature_group(
    name="my_feature_group",
    version=1,
    description="my first feature group",
    primary_key=['user_id'],
    event_time='visited_last'
    online_enabled=True
)

my_feature_group.insert(dataframe_with_feature_data)
```

## Get or Create Feature Views to Read Feature Data

You can retrieve or create `FeatureView` objects using the `get_or_create_feature_view()` method. `FeatureView`s objects are a list of selected features from one or more `FeatureGroup`s to be used as input to a model. You can use it to materialize `TrainingDataset`s or serve **Real-Time** feature data to provide up to date context to your model.

```python
my_feature_view = fs.get_or_create_feature_view(
    name="my_feature_view",
    version=1,
    description="my first feature view",
    query=my_feature_group.select(["feature1", "feature2"])
)

x_train, x_test, y_train, y_test = fv.train_test_split()
# or
real_time_feature_data = fv.get_feature_vector(entry={"user_id": 1})
```

## Get or Create Storage Connectors to access External Data

You can retrieve or create `StorageConnector` objects using the `get_or_create_storage_connector()` method. `StorageConnector`s objects are used to access external data sources, such as S3, Snowflake, BigQuery. Each `StorageConnector` has its own properties so check out the docs for the specific data source you are interested in:

- [S3](https://docs.hopsworks.ai/latest/generated/api/hsfs/#hsfs.s3_connector.S3Connector)
- [BigQuery](https://docs.hopsworks.ai/latest/generated/api/hsfs/#hsfs.bigquery_connector.BigQueryConnector)
- [Kafka](https://docs.hopsworks.ai/latest/user_guides/fs/storage_connector/creation/kafka/)
- [Snowflake](https://docs.hopsworks.ai/latest/generated/api/hsfs/#hsfs.snowflake_connector.SnowflakeConnector)

## Checkout what's available in your Feature Store

You can list all the `FeatureGroup`s and `FeatureView`s in your project using the `show_feature_groups()` and `show_feature_views()` methods. Use `with_feature=True` to list their features `FeatureGroup` or `FeatureView`.

```python
fs.show_feature_groups(with_features=True) # or fs.show_feature_views(with_features=True)
# output
# +----------------------------+----+-------+--------+----------+
# | offline_fg_with_complex_ft | v1 | 11339 | Stream | 🔴 Batch |
# +----------------------------+----+-------+--------+----------+
#    Features :
#      * id        bigint            (primary key)
#      * cc_num    bigint
#      * when      timestamp         (event-time)
#      * array_ft  array <double>
# +---------------------+----+------+--------+--------------+
# | prices_composite_fg | v3 | 7300 | Stream | 🟢 Real-Time |
# +---------------------+----+------+--------+--------------+
#    Features :
#      * ticker         string       (primary key)
#      * ticker_number  bigint       (primary key)
#      * when           timestamp    (event-time)
#      * price          bigint
```
