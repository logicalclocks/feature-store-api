# Query vs DataFrame

HSFS provides a DataFrame API to ingest data into the Hopsworks Feature Store. You can also retrieve feature data in a DataFrame, that can either be used directly to train models or [materialized to file(s)](training_dataset.md) for later use to train models.

The idea of the Feature Store is to have pre-computed features available for both training and serving models. The key functionality required to generate training datasets from reusable features are: feature selection, joins, filters and point in time queries. To enable this functionality, we are introducing a new expressive Query abstraction with `HSFS` that provides these operations and guarantees reproducible creation of training datasets from features in the Feature Store.

The new joining functionality is heavily inspired by the APIs used by Pandas to merge DataFrames. The APIs allow you to specify which features to select from which feature group, how to join them and which features to use in join conditions.

=== "Python"
    ```python
    # create a query
    feature_join = rain_fg.select_all() \
                            .join(temperature_fg.select_all(), on=["date", "location_id"]) \
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

=== "Scala"
    ```scala
    # create a query
    val featureJoin = (rainFg.selectAll()
                            .join(temperatureFg.selectAll(), on=Seq("date", "location_id"))
                            .join(locationFg.selectAll()))

    val td = (fs.createTrainingDataset()
                            .name("rain_dataset")
                            .version(1)
                            .label(”weekly_rain”)
                            .dataFormat(”tfrecords”)
                            .build())

    # materialize query in the specified file format
    td.save(featureJoin)

    # use materialized training dataset for training, possibly in a different environment
    val td = fs.getTrainingDataset(“rain_dataset”, 1)

    # reproduce query for online feature store and drop label for inference
    val jdbcQuerystring = td.getQuery(true, false)
    ```

If a data scientist wants to modify a new feature that is not available in the Feature Store, she can write code to compute the new feature (using existing features or external data) and ingest the new feature values into the Feature Store. If the new feature is based solely on existing feature values in the Feature Store, we call it a derived feature. The same HSFS APIs can be used to compute derived features as well as features using external data sources.

## The Query Abstraction

Most operations performed on `FeatureGroup` metadata objects will return a `Query` with the applied operation.

### Examples

Selecting features from a feature group is a lazy operation, returning a query with the selected
features only:

=== "Python"
    ```python
    rain_fg = fs.get_feature_group("rain_fg")

    # Returns Query
    feature_join = rain_fg.select(["location_id", "weekly_rainfall"])
    ```

=== "Scala"
    ```Scala
    val rainFg = fs.getFeatureGroup("rain_fg")

    # Returns Query
    val featureJoin = rainFg.select(Seq("location_id", "weekly_rainfall"))
    ```

#### Join

Similarly joins return queries. The simplest join, is one of two feature groups without specifying a join key or type.
By default Hopsworks will use the maximal matching subset of the primary key of the two feature groups as joining key, if not specified otherwise.

=== "Python"
    ```python
    # Returns Query
    feature_join = rain_fg.join(temperature_fg)
    ```

=== "Scala"
    ```Scala
    # Returns Query
    val featureJoin = rainFg.join(temperatureFg)
    ```
More complex joins are possible by selecting subsets of features from the joines feature groups and by specifying a join key and type.
Possible join types are "inner", "left" or "right". Furthermore, it is possible to specify different features for the join key of the left and right feature group.
The join key lists should contain the name of the features to join on.

=== "Python"
    ```python
    feature_join = rain_fg.select_all() \
                            .join(temperature_fg.select_all(), on=["date", "location_id"]) \
                            .join(location_fg.select_all(), left_on=["location_id"], right_on=["id"], how="left")
    ```

=== "Scala"
    ```scala
    val featureJoin = (rainFg.selectAll()
                            .join(temperatureFg.selectAll(), Seq("date", "location_id"))
                            .join(locationFg.selectAll(), Seq("location_id"), Seq("id"), "left"))
    ```

!!! error "Nested Joins"
    The API currently does not support nested joins. That is joins of joins.
    You can fall back to Spark DataFrames to cover these cases. However, if you have to use joins of joins, most likely
    there is potential to optimise your feature group structure.

#### Filter

In the same way as joins, applying filters to feature groups creates a query with the applied filter.

Filters are constructed with Python Operators `==`, `>=`, `<=`, `!=`, `>`, `<` and using the Bitwise Operators `&` and `|` to construct conjunctions.
For the Scala part of the API, equivalent methods are available in the `Feature` and `Filter` classes.

=== "Python"
    ```python
    filtered_rain = rain_fg.filter(rain_fg.location_id == 10)
    ```

=== "Scala"
    ```scala
    val filteredRain = rainFg.filter(rainFg.getFeature("location_id").eq(10))
    ```

Filters are fully compatible with joins:

=== "Python"
    ```python
    feature_join = rain_fg.select_all() \
                            .join(temperature_fg.select_all(), on=["date", "location_id"]) \
                            .join(location_fg.select_all(), left_on=["location_id"], right_on=["id"], how="left") \
                            .filter((rain_fg.location_id == 10) | (rain_fg.location_id == 20))
    ```

=== "Scala"
    ```scala
    val featureJoin = (rainFg.selectAll()
                            .join(temperatureFg.selectAll(), Seq("date", "location_id"))
                            .join(locationFg.selectAll(), Seq("location_id"), Seq("id"), "left")
                            .filter(rainFg.getFeature("location_id").eq(10).or(rainFg.getFeature("location_id").eq(20))))
    ```

The filters can be applied at any point of the query:

=== "Python"
    ```python
    feature_join = rain_fg.select_all() \
                            .join(temperature_fg.select_all().filter(temperature_fg.avg_temp >= 22), on=["date", "location_id"]) \
                            .join(location_fg.select_all(), left_on=["location_id"], right_on=["id"], how="left") \
                            .filter(rain_fg.location_id == 10)
    ```

=== "Scala"
    ```scala
    val featureJoin = (rainFg.selectAll()
                            .join(temperatureFg.selectAll().filter(temperatureFg.getFeature("avg_temp").ge(22)), Seq("date", "location_id"))
                            .join(locationFg.selectAll(), Seq("location_id"), Seq("id"), "left")
                            .filter(rainFg.getFeature("location_id").eq(10)))
    ```

## Methods

{{query_methods}}

## Properties

{{query_properties}}
